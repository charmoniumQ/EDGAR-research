from typing import List, Dict, Union, Tuple, NamedTuple, Iterable, Iterator
from html.parser import HTMLParser
import io
import datetime
import zipfile
import re
from edgar_code.util import download_retry


class Index(NamedTuple):
    form_type: str
    company_name: str
    CIK: int
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


def parse_header(lines: Iterator[bytes]):
    # see https://www.sec.gov/Archives/edgar/full-index/2016/QTR3/form.idx

    # the headings occur in a different order based on what index you are
    # looking at (eg. FORM TYPE comes first in the form.idx)

    # throw away the first 4 lines (just informational)
    for _ in range(4):
        next(lines)

    # throw away the next 4 lines should be blank
    for _ in range(4):
        assert next(lines).strip() == b''

    col_names = split(next(lines))

    # sometimes 'File Name' is two words
    if 'File Name' in col_names:
        # replace it with 'Filename'
        col_names[col_names.index('File Name')] = 'Filename'

    assert col_names == ['Form Type', 'Company Name', 'CIK', 'Date Filed', 'Filename']

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
            CIK=int(line_dict['CIK']),
            date_filed=datetime.datetime.strptime(
                line_dict['Date Filed'], '%Y-%m-%d'
            ).date(),
            url='https://www.sec.gov/Archives/' + line_dict['Filename'],
        )


def filter_form_type(records: Iterable[Index], this_form_type: str):
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
    elems = line.split('|')

    # too many elements, elems[2] should be part of elems[1]
    while len(elems) > 5:
        elems[1] += ' ' + elems[2]
        del elems[2]

    # too few elements, empty field present
    if len(elems) < 5:
        elems.insert(1, '')

    return elems


doc_pattern = re.compile(b'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL)
text_pattern = re.compile(b'(.*)<TEXT>(.*?)</TEXT>(.*)', re.DOTALL)
tagcontent_pattern = re.compile(b'<(.*?)>(.*?)[\n<]', re.DOTALL)
def sgml2fileinfos(sgml_contents: bytes) -> Iterable[Dict[str, str]]:
    '''Inputs the downloaded SGML and outputs the files

    Each document described in the SGML gets converted to a dict of all of its
    attributes, including the 'text', which has actual text of a document'''

    for doc_match in doc_pattern.finditer(sgml_contents):
        this_file = {}
        doc_text = doc_match.group(1)

        text_match = text_pattern.search(doc_text)
        if text_match:
            this_file['text'] = text_match.group(2).decode()
            rest_text = text_match.group(1) + text_match.group(3)

            # Match both forms
            # <TAG>stuff
            # <OTHERTAG>more stuff
            # and
            # <TAG>stuff</TAG>
            # <OTHERTAG>more stuff</OTHERTAG>
            for tagcontent in tagcontent_pattern.finditer(rest_text):
                tagname = tagcontent.group(1).lower().decode()
                content = tagcontent.group(2).decode()
                this_file[tagname] = content
            yield this_file


def find_form(fileinfos: Iterable[Dict[str, str]], form_type: str) -> str:
    '''Returns the decoded 10-K (or form_type) string from the list of
    dict of fileinfos from sgml2fileinfos

    '''
    for file_info in fileinfos:
        if file_info['type'] == form_type:
            try:
                return file_info['text']
            except UnicodeDecodeError as exc:
                raise ParseError(str(exc))
    raise ParseError(f'Cannot find the form_type {form_type}')


def is_html(text: str) -> bool:
    tags = 'p div td'.split(' ')
    tags += [tag.upper() for tag in tags]
    return any(f'</{tag}>' in text
               for tag in tags)


def clean_html(html: str) -> str:
    '''Puts newlines where they belong in HTML and removes tags'''

    # replace internal linebreaks with spaces
    # linebreaks within a tag are not treated as significant dividing marks
    html = html.replace('\n', ' ')
    html = html.replace('\r', ' ')

    # replace newline-inducing tags with linebreaks
    # tags are treated as significant dividing marks
    newline_tags = 'p div tr br h1 h2 h3 h4 h5'.split(' ')
    newline_tags += [tag.upper() for tag in newline_tags]
    for tag in newline_tags:
        find1 = f'</{tag}>'
        find2 = f'<{tag} />'
        replace = f'my-escape-newlines\n</{tag}>'
        html = html.replace(find1, replace)
        html = html.replace(find2, replace)

    # this is too egregious.
    # html = re.sub('<a.*?</a>', '', html)

    return html


html_tag_pattern = re.compile('<.*?>')
def html2text(html: str) -> str:
    '''Extract plain text from HTML'''

    # remove ALL HTML tags.
    # this makes beautiful soup take less time to parse it
    html = html_tag_pattern.sub('', html)

    # decode HTML characters such as &amp; &
    # text = BeautifulSoup(html, 'html.parser').text
    # TODO: is this necessary?
    text = HTMLParser().unescape(html)

    text = text.replace('my-escape-newlines', '\n\n')
    return text


def is_toc(alpha_line: str) -> bool:
    return ('tableofcontents' in alpha_line
            # and not much else (except the word 'page' could be on the line)
            and len(alpha_line) <= len('tableofcontents') + 4)


non_alpha_chars_pattern = re.compile('[^a-zA-Z]+')
def is_text_line(line: str) -> bool:
    # remove non-alphabetic characters
    alpha_line = non_alpha_chars_pattern.sub('', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 3 and not is_toc(alpha_line)


page_pattern = re.compile(r'\<PAGE\>.*')
page2_pattern = re.compile(r'^\s*PAGE.*')
c_pattern = re.compile(r'\<C\>\s*')
bullets = '[\u25cf\u00b7\u2022\x95]'
bullet_pattern = re.compile(fr'\s+{bullets}\s+')
spaces_pattern = re.compile(' +')
single_newline_pattern = re.compile('([^\n])\n([^\n])')
multi_newlines_pattern = re.compile('\n+')
def clean_text(text: str) -> str:
    '''Cleans plaintext for semantically insignificant items'''

    text = page_pattern.sub('\n', text)
    text = page2_pattern.sub('\n', text)
    text = c_pattern.sub('\n', text)

    #### character replacements ####

    # change windows-newline to linux-newline
    text = text.replace('\r\n', '\n')

    # change remaining carriage returns to newlines
    # text = re.sub('\r', '\n', text)
    # I don't believe there should be any remaining carriage returns
    if '\r' in text:
        print('tell sam to remove carriage returns')

    # &nbsp; -> ' '
    text = text.replace('\xa0', ' ')

    # characters that I can't figure out
    for bad_char in ['\x95', '\x97']:
        text = text.replace(bad_char, "[character sam couldn't figure out]")

    # turn bullet-point + whitespace into space
    text = bullet_pattern.sub(' ', text)
    # TODO: add punctuation if punctuation is not at the end

    # evel non-ascii quotes
    text = text.replace('”', '').replace('’', '\'')

    #### filter semantic spaces ####

    # turn tab into space
    text = text.replace('\t', ' ')

    # replace multiple spaces with a single one
    # multiple spaces is not semantically significant and it complicates regex
    # later
    text = spaces_pattern.sub(' ', text)

    #### filter semantic newlines ####

    # strip leading and trailing spaces
    # these are note semantically significant and it complicates regex later on
    text = text.replace('\n ', '\n')
    text = text.replace(' \n', '\n')

    # remove single newlines (now that lines are stripped)
    # only double newlines are semantically significant
    text = single_newline_pattern.sub('\\1 \\2', text)

    # double newline -> newline (now that single newlines are removed)
    text = multi_newlines_pattern.sub('\n', text)

    text = '\n'.join(filter(is_text_line, text.split('\n')))

    return text


def main_text2form_items(
        form_type: str,
        text: str,
) -> Dict[str, str]:
    '''
    item can EITHER be
        - the string heading
        - a tuple of (pattern, name)

    If the item is a string, then the pattern is the heading and the name is
    the heading without 'item '.
    '''

    contents = {}
    i = 0

    items = item_headers[form_type]

    # TODO: revise this loop
    while i < len(items):
        # find the ith item if it exists
        item = items[i]

        if isinstance(item, tuple):
            search_term = item[0]
        else:
            search_term = fr'(?im)^{item}.*?$'
        item_match = re.search(search_term, text)

        if item_match is None:
            # this item does not exist. skip
            i += 1
            continue

        # trim text to start
        text = text[item_match.end():]

        # find the next item that exists in the document
        for j in range(i + 1, len(items)):
            next_item = items[j]

            if isinstance(next_item, tuple):
                next_search_term = next_item[0]
            else:
                next_search_term = fr'(?im)^{next_item}'
            next_item_match = re.search(next_search_term, text)

            if next_item_match is not None:
                # next item is found
                break

        else:
            # hit the end of the list without finding a next-item
            if isinstance(item, tuple):
                name = item[1]
            else:
                name = item
            contents[name] = text
            break

        # store match
        if isinstance(item, tuple):
            name = item[1]
        else:
            name = item
        contents[name] = text[:next_item_match.start()]

        # trim text
        text = text[next_item_match.start():]

        # start at next_item
        i = j

    return contents


class ParseError(Exception):
    pass


header_pattern = re.compile('^\\s*part i[\\. \n]', re.MULTILINE | re.IGNORECASE)
def remove_header(text: str) -> str:
    # text is header, (optional) table of contents, and body
    # table of contents starts with "Part I"
    # body starts with "Part I"

    # ====== trim header ====
    # note that the [\\. \n] is necessary otherwise the regex will match
    # "part ii"
    # note that the begining of line anchor is necessary because we don't want
    # it to match "part i" in the middle of a paragraph
    print(text[1000:4000])
    parti = header_pattern.search(text)
    if parti is None:
        raise ParseError('Could not find "Part I" to remove header')
    text = text[parti.end():]

    # ====== remove table of contents, if it exists ====
    parti = header_pattern.search(text)
    if parti:
        text = text[parti.end():]
    else:
        # this means there was no table of contents
        pass
    return text


item_headers: Dict[str, List[Union[str, Tuple[re.Pattern, str]]]] = {
    '10-K': [
        'Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5',
        'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A',
        'Item 9B', 'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14',
        'Item 15', 'Signatures'],
    '8-K': [
        'Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04',
        'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05',
        'Item 2.06',
        'Item 3.01', 'Item 3.02', 'Item 3.03',
        'Item 4.01', 'Item 4.02',
        'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05',
        'Item 5.06', 'Item 5.07', 'Item 5.08',
        'Item 6.01', 'Item 6.02', 'Item 6.03', 'Item 6.04', 'Item 6.05',
        'Item 6.06',
        'Item 7.01', 'Item 8.01', 'Item 9.01',
        (re.compile('^signatures?', flags=re.IGNORECASE | re.MULTILINE), 'sig')
    ],
}
