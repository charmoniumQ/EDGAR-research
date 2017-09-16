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
        form_8k = helpers.find_form(fileinfos, '8-K')
        if helpers.is_html(form_8k):
            html = form_8k
            clean_html = helpers.clean_html(html)
            raw_text = helpers.html_to_text(clean_html)
        else:
            raw_text = form_8k
        clean_text = helpers.clean_text(raw_text)
        items = helpers.text_to_items(clean_text, item_headers)
    finally:
        if debug_path is not None:
            helpers.extras_to_disk(locals(), debug_path)
    return items


item_headers = [
    'Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04',
    'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05', 'Item 2.06', # noqa
    'Item 3.01', 'Item 3.02', 'Item 3.03',
    'Item 4.01', 'Item 4.02',
    'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05', 'Item 5.06', 'Item 5.07', 'Item 5.08', # noqa
    'Item 6.01', 'Item 6.02', 'Item 6.03', 'Item 6.04', 'Item 6.05', 'Item 6.06', # noqa
    'Item 7.01', 'Item 8.01', 'Item 9.01',
    (re.compile('^signatures?', flags=re.IGNORECASE | re.MULTILINE), 'sig')
]


__all__ = ['download']
