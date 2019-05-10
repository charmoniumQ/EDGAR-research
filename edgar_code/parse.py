from typing import List, Dict, NamedTuple, Iterable, Iterator
import html as pyhtml
import io
import datetime
import zipfile
import re
import chardet
from edgar_code.util import download_retry


class Index(NamedTuple):
    form_type: str
    company_name: str
    cik: int
    date_filed: datetime.date
    url: str
    year: int
    qtr: int


def download_index_lines(year: int, qtr: int) -> Iterable[bytes]:
    index_type = 'form'
    url = f'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/{index_type}.zip'
    compressed_str = download_retry(url)
    compressed_file = io.BytesIO(compressed_str)

    # unzip the file
    with zipfile.ZipFile(compressed_file, 'r') as index_zip:
        uncompressed_file = index_zip.open(f'{index_type}.idx')
        return uncompressed_file


def parse_header(lines: Iterator[bytes]) -> List[str]:
    # see https://www.sec.gov/Archives/edgar/full-index/2016/QTR3/form.idx

    # the headings occur in a different order based on what index you are
    # looking at (eg. FORM TYPE comes first in the form.idx)

    # throw away the first 5 lines (just informational)
    for _ in range(5):
        next(lines)

    line = b''
    while line == b'':
        line = next(lines).strip()

    col_names = split(line)

    # sometimes 'File Name' is two words
    if 'File Name' in col_names:
        # replace it with 'Filename'
        col_names[col_names.index('File Name')] = 'Filename'

    assert set(col_names) == {'Form Type', 'Company Name', 'CIK', 'Date Filed', 'Filename'}

    throw_away = next(lines)  # throw away next line

    assert set(throw_away.strip()) == set(b'-') # this line should just be dashes

    return col_names


def parse_body(
        year: int,
        qtr: int,
        lines: Iterable[bytes],
        col_names: List[str]
) -> Iterable[Index]:
    for line in lines:
        # elems_dict is a dict from col_heading (eg. 'CIK')
        # to value (eg. 1020391)
        # becauset the order is not guarunteed
        line_dict = dict(zip(col_names, split(line)))

        yield Index(
            year=year,
            qtr=qtr,
            form_type=line_dict['Form Type'],
            company_name=line_dict['Company Name'],
            cik=int(line_dict['CIK']),
            date_filed=datetime.datetime.strptime(
                line_dict['Date Filed'], '%Y-%m-%d'
            ).date(),
            url='https://www.sec.gov/Archives/' + line_dict['Filename'],
        )


def filter_form_type(
        records: Iterable[Index], this_form_type: str
) -> Iterator[Index]:
    found_section = False
    for record in records:
        if record.form_type == this_form_type:
            found_section = True
            yield record
        elif found_section:
            break
#     form_types = itertools.groupby(records, operator.attrgetter('form_type'))
#     for form_type, records in form_types:
#         if this_form_type == form_type:
#             yield from records


def split(line_bytes: bytes) -> List[str]:
    '''Returns an list of elements found on each line'''

    # turns line into a string
    line = line_bytes.decode()

    # removes trailing newline and leading spaces
    line = line.strip()

    # 'a    b    c' -> 'a|b|c'
    # some indexes are separated by spaces, others are separted by |
    # this normalizes them all to be |-separated
    line = re.sub(' {2,}', '|', line)

    # 'a|b|c' -> ['a', 'b', 'c']
    elems = [elem.strip() for elem in line.split('|')]

    # too many elements, elems[2] should be part of elems[1]
    while len(elems) > 5:
        elems[1] += ' ' + elems[2]
        del elems[2]
        raise ParseError('too many elements')

    # too few elements, empty field present
    if len(elems) < 5:
        elems.insert(1, '')
        raise ParseError('too few elements')

    return elems


def download_indexes(form_type: str, year: int, qtr: int) -> Iterable[Index]:
    lines = iter(download_index_lines(year, qtr))
    col_names = parse_header(lines)
    indexes = parse_body(year, qtr, lines, col_names)
    for index in filter_form_type(indexes, form_type):
        assert index.form_type == form_type
        assert len(index.company_name) > 4
        assert index.cik > 1000
        assert (
            datetime.date(year, 1, 1) + datetime.timedelta(days=(qtr-1) * 3 * 31)
            < index.date_filed <
            datetime.date(year, 1, 1) + datetime.timedelta(days=(qtr-0) * 3 * 31)
        )
        assert str(index.cik) in index.url
        assert index.year == year
        assert index.qtr == qtr
        yield index


def sgml2fileinfos(sgml_contents: bytes) -> Iterable[Dict[str, bytes]]:
    '''Inputs the downloaded SGML and outputs the files

    Each document described in the SGML gets converted to a dict of all of its
    attributes, including the 'text', which has actual text of a document'''

    for doc_match in re.finditer(b'<DOCUMENT>(.*?)</DOCUMENT>', sgml_contents, re.DOTALL):
        this_file = {}
        doc_text = doc_match.group(1)

        text_match = re.search(b'(.*)<TEXT>(.*?)</TEXT>(.*)', doc_text, re.DOTALL)
        if text_match:
            this_file['text'] = text_match.group(2)
            rest_text = text_match.group(1) + text_match.group(3)

            # Match both forms
            # <TAG>stuff
            # <OTHERTAG>more stuff
            # and
            # <TAG>stuff</TAG>
            # <OTHERTAG>more stuff</OTHERTAG>
            for tagcontent in re.finditer(b'<(.*?)>(.*?)[\n<]', rest_text, re.DOTALL):
                tagname = tagcontent.group(1).lower().decode()
                content = tagcontent.group(2)
                this_file[tagname] = content
            yield this_file


def find_form(fileinfos: Iterable[Dict[str, bytes]], form_type: str) -> bytes:
    '''Returns the decoded 10-K (or form_type) string from the list of
    dict of fileinfos from sgml2fileinfos

    '''
    for file_info in fileinfos:
        if file_info['type'] == form_type.encode():
            try:
                return file_info['text']
            except UnicodeDecodeError as exc:
                raise ParseError(str(exc))
    raise ParseError(f'Cannot find the form_type {form_type}')


def is_html(text: bytes) -> bool:
    # however, SGML text has some <C>-like tags; I'l lsearch for closing tags of specific varietes
    tags = b'p div td'.split(b' ')
    tags += [tag.upper() for tag in tags]
    return any(
        text.find(b'</' + tag + b'>') != -1 for tag in tags
    )


def html2paragraphs(bhtml: bytes) -> List[str]:
    '''extracts text from html doc

Puts newlines between semantic paragraphs'''

    match = re.search(b'charset="?([^"]*)"', bhtml)
    # <meta charset="UTF-8">
    # <meta http-equiv="Content-Type" content="text/html;charset=ISO-8859-1">
    if match:
        encoding = match.group(1).decode()
        html = bhtml.decode(encoding)
    else:
        html = bhtml.decode(chardet.detect(bhtml)['encoding'])

    # replace internal linebreaks with spaces
    # linebreaks within a tag are not treated as significant dividing marks
    html = html.replace('\n', ' ')
    html = html.replace('\r', ' ')

    # replace newline-inducing tags with linebreaks
    # tags are treated as significant dividing marks
    newline_tags = 'p div tr h1 h2 h3 h4 h5 title'.split(' ')
    newline_tags += [tag.upper() for tag in newline_tags]
    for tag in newline_tags:
        html = html.replace(f'</{tag}>', 'my-escape-newlines')

    # remove ALL HTML tags.
    # this makes beautiful soup take less time to parse it
    html = re.sub(r'<[^>]*>', '', html)

    # decode HTML characters such as &amp; &
    # text = BeautifulSoup(html, 'html.parser').text
    text = pyhtml.unescape(html)

    return text.split('my-escape-newlines')


def text2paragraphs(btext: bytes) -> List[str]:
    text = btext.decode(chardet.detect(btext)['encoding'])

    tags = ['PAGE', 'C', 'S', 'CAPTION']
    for tag in tags:
        text = re.sub(rf'\<{tag}\>.*', '\n', text)
    text = re.sub(r'\<TABLE\>.*?\</TABLE\>', '', text)
    text = re.sub(r'^\s*PAGE.*$', '\n', text)

    # change windows-newline to linux-newline
    # before we do word-wrap/paragraphs
    text = text.replace('\r\n', '\n')

    # change remaining carriage returns to newlines
    # text = re.sub('\r', '\n', text)
    # I don't believe there should be any remaining carriage returns
    if '\r' in text:
        raise ValueError('tell sam to remove carriage returns')

    # save paragraph breaks
    text = re.sub('\n{2,}', '<br />', text)
    # eliminate word wrap
    text = text.replace('\n', ' ')
    # restore paragraph breaks
    return text.split('<br />')


def is_text_line(line: str) -> bool:
    # remove non-alphabetic characters
    alpha_line = re.sub('[^a-zA-Z]+', '', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 3 and not (
        'tableofcontents' in alpha_line
        # and not much else (except the word 'page' could be on the line)
        and len(alpha_line) <= len('tableofcontents') + 4
    )


def clean_paragraph(paragraph: str) -> str:
    # &nbsp; -> ' '
    paragraph = paragraph.replace(' ', ' ')

    # evil non-ascii quotes
    paragraph = paragraph.replace('”', '"')
    paragraph = paragraph.replace('’', '\'')

    # turn tab into space
    paragraph = paragraph.replace('	', ' ')

    # turn bullet-point + whitespace into space
    bullets = '[\u25cf\u00b7\u2022\x95]'
    paragraph = re.sub(fr'\s+{bullets}\s+', ' ', paragraph)
    # TODO: add punctuation if punctuation is not at the end

    # replace multiple spaces with a single one
    # multiple spaces is not semantically significant and it complicates regex
    # later
    paragraph = re.sub(' {2,}', ' ', paragraph)

    # strip leading and trailing spaces
    paragraph = paragraph.strip()

    # characters that I can't figure out
    assert len(paragraph) == len(paragraph.encode('ascii'))
    return paragraph


def clean_paragraphs(paragraphs: List[str]) -> List[str]:
    return list(filter(is_text_line, map(clean_paragraph, paragraphs)))


def remove_header(paragraphs: List[str]) -> List[str]:
    # text is header, (optional) table of contents, and body
    # table of contents starts with "Part I"
    # body starts with "Part I"

    # ====== trim header ====
    # note that the [\\. \n] is necessary otherwise the regex will match
    # "part ii"
    # note that the begining of line anchor is necessary because we don't want
    # it to match "part i" in the middle of a paragraph
    parti_pattern = re.compile('part i.?', re.IGNORECASE)
    partis = [
        i for i, paragraph in enumerate(paragraphs)
        if parti_pattern.match(paragraph)
    ]
    if not partis:
        raise ParseError('Could not find "Part I" to remove header')
    elif len(partis) == 1:
        return paragraphs[partis[0]:]
    elif len(partis) == 2:
        return paragraphs[partis[1]:]
    else:
        raise ParseError('Found too many "Part I"s to remove')


def paragraphs2rf(
        paragraphs: List[str],
        pre_2006: bool,
) -> List[str]:
    if pre_2006:
        start_pattern = re.compile(
            'item 7.? ?(management\'s discussion and '
            'analysis of financial condition and results of operations)$',
            re.IGNORECASE
        )
        stop_pattern = re.compile(
            'item 8.? ?(financial statements and '
            'supplementary data)?$',
            re.IGNORECASE
        )
    else:
        start_pattern = re.compile('^item 1a.? ?(risk factors.*)?$', re.IGNORECASE)
        stop_pattern = re.compile('^item 2.? ?(properties.*)?$', re.IGNORECASE)
    start = [
        i for i, paragraph in enumerate(paragraphs)
        if start_pattern.match(paragraph)
    ]
    stop = [
        i for i, paragraph in enumerate(paragraphs)
        if stop_pattern.match(paragraph)
    ]
    if len(start) != 1:
        raise ParseError(f'got {len(start)} starts')
    if len(stop) != 1:
        raise ParseError(f'got {len(stop)} stops')
    else:
        return paragraphs[start[0]+1:stop[0]]


class ParseError(Exception):
    pass
