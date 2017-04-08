from mining.cache import download
from mining.retrieve_10k import SGML_to_files, extract_to_disk, html_to_text, ParseError
import re


def get_departure_of_directors_or_certain_officers(path, enable_cache, verbose, debug, throw=True, wpath=''):
    try:
        items = get_items(path, enable_cache, verbose, debug, wpath)
        if '5.02' in items:
            return items['5.02']
        else:
            raise ParseError('Item 5.02 not found')
    except Exception as e:
        if throw:
            raise e
        else:
            return


def get_items(path, enable_cache, verbose, debug, wpath=''):
    sgml = download(path, enable_cache, verbose, debug)
    if verbose: print('retrieve_8k.py: started parsing SGML')
    files = SGML_to_files(sgml, verbose, debug)
    if verbose: print('retrieve_8k.py: started parsing HTML')
    items = parse_8k(files, enable_cache, verbose, debug, path)
    if verbose: print('retrieve_8k.py: finished parsing')
    return items


def get_8k(path, enable_cache, verbose, debug):
    sgml = download(path, enable_cache, verbose, debug)
    files = SGML_to_files(sgml, verbose, debug)
    item = parse_8k(path, enable_cache, verbose, debug, path)
    return item


def parse_8k(files, enable_cache, verbose, debug, path = ''):
    # files is a list of dictionary which has 'type' and 'text' as items in the dictionary
    for file in files:
        if file['type'].lower() == "8-k":
            break
        
    html = file['text']
    text = html_to_text(html, verbose, debug, path)
    text = clean_text(text, verbose, False, '')

    items_8k = text_to_items(text, verbose, debug, path)

    return items_8k


def extract_8k(directory, path,enable_cache,verbose,debug):
    sgml = download(path,enable_cache,verbose,debug)
    files = SGML_to_files(sgml, verbose, debug)
    extract_to_disk(directory, files, verbose, debug)

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

    if verbose: print('cleaned size', len(text))
    return text

items = ['Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04',
         'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05', 'Item 2.06',
         'Item 3.01', 'Item 3.02', 'Item 3.03',
         'Item 4.01', 'Item 4.02',
         'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05', 'Item 5.06', 'Item 5.07', 'Item 5.08',
         'Item 6.01', 'Item 6.02', 'Item 6.03', 'Item 6.04', 'Item 6.05', 'Item 6.06',
         'Item 7.01', 'Item 8.01', 'Item 9.01']

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
            contents[item.lower().replace("item ", "")] = text
            break

        # store match
        contents[item.lower().replace("item ", "")] = text[:next_item_match.start()]
        # trim text
        text = text[next_item_match.start():]
        # start at next_item
        i = j
    return contents

