from __future__ import print_function
import re
import itertools
import mining.cache as cache
import mining.parsing as parsing

def get_risk_factors(path, enable_cache, throw=False):
    '''Returns the Item 1A section of the 10k form

if throw is False, then errors will fail silently and an empty string will be returned
'''
    try:
        items = get_10k_items(path, enable_cache)
        if '1a' in items:
            return items['1a']
        else:
            raise parsing.ParseError('Item 1A not found')
    except Exception as exc:
        if throw:
            raise exc
        else:
            return ''

item_headers = [
    'Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5',
    'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A', 'Item 9B',
    'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14', 'Item 15',
    'Signatures']

def get_10k_items(edgar_path, enable_cache, debug_path=None):
    '''Returns a dict of items from the 10-K

The keys will be a lower-cased item header without the word 'item', such as
'1a' or '5'.
If path is not None, it should be the pathlib.Path of an extant directory.
Debug variables will be written to that path
'''
    try:
        sgml = cache.download(edgar_path, enable_cache)
        fileinfos = parsing.SGML_to_fileinfos(sgml)
        form_10k = parsing.find_form(fileinfos, '10-K')
        if parsing.is_html(form_10k):
            html = form_10k
            clean_html = parsing.clean_html(html)
            raw_text = parsing.html_to_text(clean_html)
        else:
            raw_text = form_10k
        clean_text = parsing.clean_text(raw_text)
        main_text = remove_header(clean_text)
        items = parsing.text_to_items(main_text, item_headers)
    finally:
        if debug_path is not None:
            extras_to_disk(locals(), debug_path)
    return items


def remove_header(text):
    # text is header, (optional) table of contents, and body
    # table of contents starts with "Part I"
    # body starts with "Part I"

    ### trim header
    # note that the [\. \n] is necessary otherwise the regex will match "part ii"
    # note that the begining of line anchor is necessary because we don't want
    # it to match "part i" in the middle of a paragraph
    parti = re.search('^part i[\\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti is None:
        raise parsing.ParseError('Could not find "Part I" to remove header')
    text = text[parti.end():]

    ### remove table of contents, if it exists
    parti = re.search('^part i[\\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti:
        text = text[parti.end():]
    else:
        # this means there was no table of contents
        pass
    return text

def extras_to_disk(extras, path):
    '''Writes debug variables from to disk from get_10k_items'''

    if path.exists():
        for i in itertools.count(2):
            path = path.with_name(path.name + '_' + str(i))
            if not path.exists():
                break
    path.mkdir()

    if 'items' in extras:
        items_list = [key + '\n----\n' + val for key, val in extras['items'].items()]
        extras['items_str'] = '\n----\n'.join(items_list)
    parsing.dict_to_disk(extras, path)

    if 'fileinfos' in extras:
        raw_dir = path / 'raw_form'
        raw_dir.mkdir()
        parsing.fileinfos_to_disk(extras['fileinfos'], raw_dir)
