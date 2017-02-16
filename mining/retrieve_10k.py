from __future__ import print_function
from os import mkdir
from os.path import join
import re
from mining.cache import download
from bs4 import BeautifulSoup

VERBOSE = True
DEBUG = True

def html_to_text(textin, debug):
    '''Extract real text from HTML, after removing table of contents'''

    textin = textin.decode()

    # replace line breaks within elements with spaces
    textin = textin.replace('\n', ' ')

    # replace line breaks
    newline_tags = 'p div tr P DIV TR'.split(' ')
    for tag in newline_tags:
        textin = textin.replace('<' + tag, '\n<' + tag)

    # parse as HTML and extract text
    html_10k = BeautifulSoup(textin, 'html.parser')
    text = html_10k.text

    # &nbsp -> ' '
    text = text.replace('\xa0', ' ')


    # replace multiple spaces with a single one
    text = re.sub('  +', ' ', text)

    try:
        # remove header
        parti = re.search('^ ?part i[\. \n]', text, re.MULTILINE | re.IGNORECASE)
        text = text[parti.end():]
        
        # remove table of contents
        parti = re.search('^ ?part i[\. \n]', text, re.MULTILINE | re.IGNORECASE)
        if parti:
            text = text[parti.end():]
        else:
            # this means there was no table of contents
            pass
    except:
        if DEBUG and debug:
            with open('results/10k_error.txt', 'w') as f:
                f.write(text)
        raise ParseError('Could not find part 1 (removing table of contents)')
    return text

items = ['Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5', 'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A', 'Item 9B', 'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14', 'Item 15', 'Signatures']
names = '1 1A 1B 2 3 4 5 6 7 7A 8 9 9A 9B 10 11 12 13 14 15'.split(' ')

def text_to_items(text, debug):
    contents = {}
    for name, item, next_item in zip(names, items[:-1], items[1:]):

        # search for text between {item} and {next_item}
        item_pattern = re.compile(r'^\s*({item}.*?)$(.*?)(?=^\s*{next_item})'.format(**locals()), re.DOTALL | re.MULTILINE | re.IGNORECASE)
        match = item_pattern.search(text)

        # print a message on failure
        if not match:
            if DEBUG and debug:
                pass
                print('{DEBUG} {debug}'.format(**locals()))
                with open('results/10k_error_{item}.txt'.format(**locals()), 'w') as f:
                    f.write(text)
            if VERBOSE:
                print('retrieve_10k.py: Could not find {item}'.format(**locals()))
        else:

            # store contents of match
            contents[name] = match.group(2)

            # chop text after match, so that future searches only search after this point
            text = text[match.end():]
    return contents

def parse_10k(files, debug):
    '''Inputs a list of dicts (one dict for each file) aad returns a dict mapping from names (declared above) to strings of content'''
    for file_info in files:
        if file_info['type'] == '10-K':
            break
    else:
        raise ParseError('Cannot find the 10K')

    text = html_to_text(file_info['text'], debug)
    if VERBOSE: print('retrieve_10k.py: Normalized text...')
    return text_to_items(text, debug)


def extract_to_disk(directory, files):
    '''Extracts all files in the list of dicts (one for each file) into the directory for manual examination'''
    mkdir(directory)
    for file in files:
        if file['type'] == '10-K':
            print(file['filename'])
        try:
            dfname = join(directory, file['filename'])
            with open(dfname, 'wb') as f:
                f.write(file['text'])
        except:
            print(file.keys())

def SGML_to_files(sgml_contents):
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

def get_risk_factors(path, enable_cache=True, debug=True):
    sgml = download(path, enable_cache)
    files = SGML_to_files(sgml)
    if VERBOSE: print('retrieve_10k.py: started parsing')
    risk_factors = parse_10k(files, debug)
    if VERBOSE: print('retrieve_10k.py: finished parsing')
    if '1A' in risk_factors:
        return risk_factors['1A']
    else:
        raise ParseError('Item 1A not found')

class ParseError(Exception):
    pass

# Rox
# OpenMPI
# RabitMQ

# 46 604
