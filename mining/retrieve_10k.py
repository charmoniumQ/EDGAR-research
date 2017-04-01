from __future__ import print_function
from os import mkdir
from os.path import join
import re
from mining.cache import download
from bs4 import BeautifulSoup

def get_risk_factors(path, enable_cache, verbose, debug, throw=True, wpath=''):
    try:
        items = get_items(path, enable_cache, verbose, debug, wpath)
        if '1A' in items:
            return items['1A']
        else:
            raise ParseError('Item 1A not found')
    except Exception as e:
        if throw:
            raise e
        else:
            return

def get_items(path, enable_cache, verbose, debug, wpath=''):
    sgml = download(path, enable_cache, verbose, debug)
    if verbose: print('retrieve_10k.py: started parsing SGML')
    files = SGML_to_files(sgml, verbose, debug)
    if verbose: print('retrieve_10k.py: started parsing HTML')
    items = parse_10k(files, verbose, debug, wpath)
    if verbose: print('retrieve_10k.py: finished parsing')
    return items

def SGML_to_files(sgml_contents, verbose, debug):
    '''Inputs the downloaded SGML and outputs a list of dicts (one dict for each file)

    Each document described in the SGML gets converted to a dict of all of its attributes, including the 'text', which has actual text of a document'''
    doc_pattern = re.compile(b'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL)
    files = []
    for doc_match in doc_pattern.finditer(sgml_contents):
        doc_text = doc_match.group(1)
        files.append({})

        text_pattern = re.compile(b'(.*)<TEXT>(.*?)</TEXT>(.*)', re.DOTALL)
        text_match = text_pattern.search(doc_text)
        files[-1]['text'] = text_match.group(2)
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
            files[-1][tagname] = content
    return files

def extract_to_disk(directory, files, verbose, debug):
    '''Extracts all files in the list of dicts (one for each file) into the directory for manual examination'''
    mkdir(directory)
    for file in files:
        if file['type'] == '10-K':
            if verbose: print('Main 10k file', file['filename'])
        try:
            dfname = join(directory, file['filename'])
            with open(dfname, 'wb') as f:
                f.write(file['text'])
        except Exception as e:
            if verbose: print(file.keys())
            raise e

def parse_10k(files, verbose, debug, path=''):
    '''Inputs a list of dicts (one dict for each file) aad returns a dict mapping from names (declared above) to strings of content'''
    for file_info in files:
        if file_info['type'] == '10-K':
            break
    else:
        raise ParseError('Cannot find the 10K')

    if debug:
        with open(path + 'html_text.txt', 'wb') as f: f.write(file_info['text'])

    if b'</html>' in file_info['text'] or b'</HTML>' in file_info['text'] or b'</p>' in file_info['text'] or b'</P>' in file_info['text']:
        html = file_info['text']
        text = html_to_text(html, verbose, debug, path)
    else:
        text = file_info['text'].decode()

    if debug:
        with open(path + 'raw_text.txt', 'w', encoding='utf-8') as f: f.write(text)
    if verbose: print('retrieve_10k.py: Normalized html...')

    text = clean_text(text, verbose, debug, path)

    if debug:
        with open(path + 'clean_text.txt', 'w', encoding='utf-8') as f: f.write(text)
    if verbose: print('retrieve_10k.py: Normalized text...')

    items_10k = text_to_items(text, verbose, debug, path)

    if debug:
        with open(path + 'items_text.txt', 'w', encoding='utf-8') as f:
            for item, text in sorted(items_10k.items()):
                f.write(item + '\n' + text + '\n')

    return items_10k

def html_to_text(textin, verbose, debug, path=''):
    '''Extract plain text from HTML'''
    textin = textin.decode()

    # replace internal linebreaks with spaces
    # linebreaks within a tag are not treated as significant dividing marks
    textin = textin.replace('\n', ' ')
    textin = textin.replace('\r', ' ')

    # replace newline-inducing tags with linebreaks
    # tags are treated as significant dividing marks
    newline_tags = 'p div tr br P DIV TR BR'.split(' ')
    for tag in newline_tags:
        textin = textin.replace('</{tag}>'.format(**locals()), 'my-escape-newlines\n</{tag}>'.format(**locals()))
        textin = textin.replace('<{tag} />'.format(**locals()), 'my-escape-newlines\n<{tag} />'.format(**locals()))

    if debug:
        with open(path + 'clean_html_text.txt', 'w') as f: f.write(textin)

    # remove ALL HTML tags.
    # this makes beautiful soup take less time to parse it
    textin = re.sub('<.*?>', '', textin)
    for bad_html_char in ['\&#149;', '\&#151;']:
        textin = textin.replace(bad_html_char, "[weird character sam couldn't figure out]")
    # parse as HTML and extract text

    html_10k = BeautifulSoup(textin, 'html.parser')
    text = html_10k.text

    # &nbsp; -> ' '
    text = text.replace('\xa0', ' ')
    text = text.replace('my-escape-newlines', '\n\n')
    return text

def clean_text(text, verbose, debug, path=''):
    if verbose: print('starting size', len(text))

    # replace multiple spaces with a single one
    # multiple spaces is not semantically significant and it complicates regex later
    text = re.sub('\t', ' ', text)

    # turn bullet-point + whitespace + text to bulletpoint + space + text
    bullets = '([\u25cf\u00b7\u2022])'
    text = re.sub(bullets + r'\s+', '\n ', text)

    # ya know...
    text = re.sub('\r', '\n', text)

    # strip leading and trailing spaces (now that mulitiple spaces arec collapsed and \r -> \n)
    # these are note semantically significant and it complicates regex later on
    text = re.sub('\n ', '\n', text)
    text = re.sub(' \n', '\n', text)    

    # remvoe single newlines (now that lines are stripped)
    # only double newlines are semantically significant
    text = re.sub('([^\n])\n([^\n])', '\\1\\2', text)

    # double newline -> newline (now that single newlines are removed)
    text = re.sub('\n+', '\n', text)


    text = re.sub('  +', ' ', text)

    # text is header, optional table of contents, anf body
    # table of contents starts with "Part I"
    # body starts with "Part I"

    # trim header
    # note that the [\. \n] is necessary otherwise the regex will match "part ii"
    # note that the begining of line anchor is necessary because 
    parti = re.search('^part i[\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti is None:
        if debug:
            with open(path + '10k_error.txt', 'w', encoding='utf-8') as f:
                f.write(text)
        raise ParseError('Could not find "Part I" to remove header')
    text = text[parti.end():]

    # remove table of contents, if it exists
    parti = re.search('^part i[\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti:
        text = text[parti.end():]
    else:
        # this means there was no table of contents
        pass

    if verbose: print('cleaned size', len(text))
    return text

items = ['Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5', 'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A', 'Item 9B', 'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14', 'Item 15', 'Signatures']
names = '1 1A 1B 2 3 4 5 6 7 7A 8 9 9A 9B 10 11 12 13 14 15 S'.split(' ')

def text_to_items(text, verbose, debug, path=''):
    contents = {}
    i = 0
    while i < len(items):

        # find the ith item if it exists
        item = items[i]
        item_match = re.search(r'^{item}.*?$'.format(**locals()), text, re.MULTILINE | re.IGNORECASE)
        if item_match is None:
            if debug:
                print('no', item)
                with open(path + item.lower() + '.txt', 'w', encoding='utf-8') as f:
                    f.write(text)

            # this item does not exist. skip
            i += 1
            continue
        # trim text to start
        text = text[item_match.end():]

        # find the next item that exists in the document
        for j in range(i + 1, len(items)):
            next_item = items[j]
            next_item_match = re.search(r'^{next_item}'.format(**locals()), text, re.MULTILINE | re.IGNORECASE)
            if next_item_match is not None:
                # next item is found
                break
        else:
            # hit the end of the list without finding a next-item
            contents[names[i]] = text
            break

        # store match
        contents[names[i]] = text[:next_item_match.start()]
        # trim text
        text = text[next_item_match.start():]
        # start at next_item
        i = j
    return contents

class ParseError(Exception):
    pass
