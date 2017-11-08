import re
import urllib
import toolz
from . import helpers
from ..util import sanitize_fname, unused_fname


def index_to_url(form_type, index):
    sgml = urllib.request.urlopen(index.url).read()    
    fileinfos = helpers.SGML_to_fileinfos(sgml)
    for file_info in fileinfos:
        if file_info['type'] == form_type:
            filename = file_info['filename']
            break
    else:
        raise RuntimeError('no form found')
    url = index.url.replace('-', '')
    return url[:-4] + '/' + filename

@toolz.curry
def index_to_form_text(form_type, index):
    sgml = urllib.request.urlopen(index.url).read()
    fileinfos = helpers.SGML_to_fileinfos(sgml)
    return helpers.find_form(fileinfos, form_type)


@toolz.curry
def form_text_to_main_text(form_type, form_raw):
    if helpers.is_html(form_raw):
        html = form_raw
        clean_html = helpers.clean_html(html)
        raw_text = helpers.html_to_text(clean_html)
    else:
        raw_text = form_raw
    clean_text = helpers.clean_text(raw_text)
    if form_type == '10-K':
        # special case, 10-K forms contain table of contents
        try:
            main_text = helpers.remove_header(clean_text)
        except:
            main_text = None
    else:
        main_text = clean_text
    return main_text


@toolz.curry
def main_text_to_form_items(form_type, main_text):
    if main_text is not None:
        return helpers.main_text_to_form_items(main_text, item_headers[form_type])
    else:
        return {}


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
