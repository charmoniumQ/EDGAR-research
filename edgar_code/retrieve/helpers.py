import random
from html.parser import HTMLParser
import itertools
import re


def SGML_to_fileinfos(sgml_contents):
    '''Inputs the downloaded SGML and outputs a list of dicts

    Each document described in the SGML gets converted to a dict of all of its
    attributes, including the 'text', which has actual text of a document'''

    doc_pattern = re.compile(b'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL)
    for doc_match in doc_pattern.finditer(sgml_contents):
        this_file = {}
        doc_text = doc_match.group(1)

        text_pattern = re.compile(b'(.*)<TEXT>(.*?)</TEXT>(.*)', re.DOTALL)
        text_match = text_pattern.search(doc_text)
        this_file['text'] = text_match.group(2)
        rest_text = text_match.group(1) + text_match.group(3)

        # Match both forms
        # <TAG>stuff
        # <OTHERTAG>more stuff
        # and
        # <TAG>stuff</TAG>
        # <OTHERTAG>more stuff</OTHERTAG>
        tagcontent_pattern = re.compile(b'<(.*?)>(.*?)[\n<]', re.DOTALL)
        for tagcontent in tagcontent_pattern.finditer(rest_text):
            tagname = tagcontent.group(1).lower().decode()
            content = tagcontent.group(2).decode()
            this_file[tagname] = content
        yield this_file


def find_form(fileinfos, form_type):
    '''Returns the decoded 10-K string from the list of dict of fileinfos from
    SGML_to_fileinfos'''
    for file_info in fileinfos:
        if file_info['type'] == form_type:
            return file_info['text'].decode()
    else:
        raise ParseError(f'Cannot find the form_type {form_type}')


def is_html(text):
    tags = 'p div td'.split(' ')
    tags += [tag.upper() for tag in tags]
    return any(f'</{tag}>' in text
               for tag in tags)


def clean_html(html):
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


def html_to_text(html):
    '''Extract plain text from HTML'''

    # remove ALL HTML tags.
    # this makes beautiful soup take less time to parse it
    html = re.sub('<.*?>', '', html)

    # decode HTML characters such as &amp; &
    # text = BeautifulSoup(html, 'html.parser').text
    text = HTMLParser().unescape(html)

    text = text.replace('my-escape-newlines', '\n\n')
    return text


def is_toc(alpha_line):
    return ('tableofcontents' in alpha_line
            # and not much else (except the word 'page' could be on the line)
            and len(alpha_line) <= len('tableofcontents') + 4)


def is_text_line(line):
    # remove non-alphabetic characters
    alpha_line = re.sub('[^a-zA-Z]', '', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 3 and not(is_toc(alpha_line))


def clean_text(text):
    '''Cleans plaintext for semantically insignificant items'''

    if '<PAGE>' in text or '<C>' in text:
        text = re.sub('\\<PAGE\\>\s*', '\n', text)
        text = re.sub('\\<C\\>\s*', '\n', text)

    #### character replacements ####

    # change windows-newline to linux-newline
    text = re.sub('\r\n', '\n', text)

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
    bullets = '([\u25cf\u00b7\u2022\x95])'
    text = re.sub(fr'\s+{bullets}\s+', ' ', text)
    # TODO: add punctuation if punctuation is not at the end

    #### filter semantic spaces ####

    # turn tab into space
    text = re.sub('\t', ' ', text)

    # replace multiple spaces with a single one
    # multiple spaces is not semantically significant and it complicates regex
    # later
    text = re.sub('  +', ' ', text)

    #### filter semantic newlines ####

    # strip leading and trailing spaces
    # these are note semantically significant and it complicates regex later on
    text = re.sub('\n ', '\n', text)
    text = re.sub(' \n', '\n', text)

    # remove single newlines (now that lines are stripped)
    # only double newlines are semantically significant
    text = re.sub('([^\n])\n([^\n])', '\\1 \\2', text)

    # double newline -> newline (now that single newlines are removed)
    text = re.sub('\n+', '\n', text)

    text = '\n'.join(filter(is_text_line, text.split('\n')))

    return text


def main_text_to_form_items(text, items):
    '''
    item can EITHER be
        - the string heading
        - a tuple of (pattern, name)

    If the item is a string, then the pattern is the heading and the name is
    the heading without 'item '.
    '''

    contents = {}
    i = 0

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


def remove_header(text):
    # text is header, (optional) table of contents, and body
    # table of contents starts with "Part I"
    # body starts with "Part I"

    # ====== trim header ====
    # note that the [\\. \n] is necessary otherwise the regex will match
    # "part ii"
    # note that the begining of line anchor is necessary because we don't want
    # it to match "part i" in the middle of a paragraph
    parti = re.search('^part i[\\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti is None:
        raise ParseError('Could not find "Part I" to remove header')
    text = text[parti.end():]

    # ====== remove table of contents, if it exists ====
    parti = re.search('^part i[\\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti:
        text = text[parti.end():]
    else:
        # this means there was no table of contents
        pass
    return text
