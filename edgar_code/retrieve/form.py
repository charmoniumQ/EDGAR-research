import re
import urllib
import toolz
from . import helpers
from ..util import sanitize_fname, unused_fname


@toolz.curry
def indexes_to_forms(form_type, indexes, debug_dir=None):
    '''Bag of records -> bag of key-values, where the key is the record and the
value is a dict whose entries are form-items.

See download_form
'''
    return (
        indexes
        .key_to_value(index_to_form(form_type, debug_dir=debug_dir))
    )


@toolz.curry
def index_to_form(form_type, index, debug_dir=None):
    '''Returns a dict of items from the 10-K

The keys will be a lower-cased item header without the word 'item', such as
'1a' or '5'.
If debug_dir is not None, it should be the pathlib.Path of an extant directory.
Debug variables will be written to a subdirectory that path
'''
    try:
        sgml = urllib.request.urlopen(index.url).read()
        fileinfos = helpers.SGML_to_fileinfos(sgml)
        form_raw = helpers.find_form(fileinfos, form_type)
        if helpers.is_html(form_raw):
            html = form_raw
            clean_html = helpers.clean_html(html)
            raw_text = helpers.html_to_text(clean_html)
        else:
            raw_text = form_raw
        clean_text = helpers.clean_text(raw_text)
        if form_type == '10-K':
            # special case, 10-K forms contain table of contents
            main_text = remove_header(clean_text)
        else:
            main_text = clean_text
        items = helpers.text_to_items(main_text, item_headers[form_type])
    finally:
        if debug_dir is not None:
            subdir_basename = sanitize_fname(index.company_name)
            subdir = unused_fname(debug_dir, subdir_basename)
            helpers.extras_to_disk(locals(), subdir)
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


item_headers = {
    '10-K': [
        'Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5',
        'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A',
        'Item 9B', 'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14',
        'Item 15', 'Signatures'],
    '8-K': [
        'Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04',
        'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05', 'Item 2.06', # noqa
        'Item 3.01', 'Item 3.02', 'Item 3.03',
        'Item 4.01', 'Item 4.02',
        'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05', 'Item 5.06', 'Item 5.07', 'Item 5.08', # noqa
        'Item 6.01', 'Item 6.02', 'Item 6.03', 'Item 6.04', 'Item 6.05', 'Item 6.06', # noqa
        'Item 7.01', 'Item 8.01', 'Item 9.01',
        (re.compile('^signatures?', flags=re.IGNORECASE | re.MULTILINE), 'sig')
    ],
}
