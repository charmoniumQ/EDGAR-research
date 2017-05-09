import re
import itertools
import mining.cache as cache
import mining.parsing as parsing

def get_departure_of_directors_or_certain_officers(path, enable_cache):
    try:
        items = get_10k_items(path, enable_cache)
        if '5.02' in items:
            return items['5.02']
        else:
            raise parsing.ParseError('Item 1A not found')
    except Exception as e:
        if throw:
            raise e
        else:
            return None

item_headers = ['Item 1.01', 'Item 1.02', 'Item 1.03', 'Item 1.04',
         'Item 2.01', 'Item 2.02', 'Item 2.03', 'Item 2.04', 'Item 2.05', 'Item 2.06',
         'Item 3.01', 'Item 3.02', 'Item 3.03',
         'Item 4.01', 'Item 4.02',
         'Item 5.01', 'Item 5.02', 'Item 5.03', 'Item 5.04', 'Item 5.05', 'Item 5.06', 'Item 5.07', 'Item 5.08',
         'Item 6.01', 'Item 6.02', 'Item 6.03', 'Item 6.04', 'Item 6.05', 'Item 6.06',
         'Item 7.01', 'Item 8.01', 'Item 9.01',
         (re.compile('^signatures?', flags=re.IGNORECASE | re.MULTILINE), 'sig')
]


def get_8k_items(edgar_path, enable_cache, debug_path=None):
    '''Returns a dict of items from the 10-K

The keys will be a lower-cased item header without the word 'item', such as
'1a' or '5'.
If path is not None, it should be the pathlib.Path of an extant directory.
Debug variables will be written to that path
'''
    try:
        sgml = cache.download(edgar_path, enable_cache)
        fileinfos = parsing.SGML_to_fileinfos(sgml)
        form_8k = parsing.find_form(fileinfos, '8-K')
        if parsing.is_html(form_8k):
            html = form_8k
            clean_html = parsing.clean_html(html)
            raw_text = parsing.html_to_text(clean_html)
        else:
            raw_text = form_8k
        clean_text = parsing.clean_text(raw_text)
        items = parsing.text_to_items(clean_text, item_headers)
    finally:
        if debug_path is not None:
            extras_to_disk(locals(), debug_path)
    return items

def extras_to_disk(extras, path):
    if path.exists():
        for i in itertools.count(2):
            path2 = path.with_name(path.name + '_' + str(i))
            if not path2.exists():
                path = path2
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
