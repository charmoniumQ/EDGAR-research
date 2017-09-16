from __future__ import print_function
import re
import urllib
from . import helpers


def download(record, debug_path=None):
    '''Returns a dict of items from the 10-K

The keys will be a lower-cased item header without the word 'item', such as
'1a' or '5'.
If path is not None, it should be the pathlib.Path of an extant directory.
Debug variables will be written to that path
'''
    try:
        sgml = urllib.request.urlopen(record.url).read()
        fileinfos = helpers.SGML_to_fileinfos(sgml)
        form_10k = helpers.find_form(fileinfos, '10-K')
        if helpers.is_html(form_10k):
            html = form_10k
            clean_html = helpers.clean_html(html)
            raw_text = helpers.html_to_text(clean_html)
        else:
            raw_text = form_10k
        clean_text = helpers.clean_text(raw_text)
        main_text = remove_header(clean_text)
        items = helpers.text_to_items(main_text, item_headers)
    finally:
        if debug_path is not None:
            helpers.extras_to_disk(locals(), debug_path)
    return items


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
        raise helpers.ParseError('Could not find "Part I" to remove header')
    text = text[parti.end():]

    # ====== remove table of contents, if it exists ====
    parti = re.search('^part i[\\. \n]', text, re.MULTILINE | re.IGNORECASE)
    if parti:
        text = text[parti.end():]
    else:
        # this means there was no table of contents
        pass
    return text


item_headers = [
    'Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5',
    'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A', 'Item 9B',
    'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14', 'Item 15',
    'Signatures']


__all__ = ['download']
