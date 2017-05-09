import json
import itertools
import inspect
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_10k_items
from mining.retrieve_8k import get_8k_items
from util.new_directory import new_directory

year = 2016
qtr = 3
form_type = '10-k' # user lower-case

def predicate(index_info):
    # index_info['Company Name']
    # index_info['CIK']
    # i
    return index_info['Company Name'] == 'AMERICAN SOFTWARE INC'

base_dir = new_directory()
print('Results in ' + base_dir.as_posix())

with (base_dir / 'info.txt').open('w+') as f:
    predicate_code = ''.join(inspect.getsourcelines(predicate)[0])
    f.write('''
script: {__file__}
year: {year}
qtr: {qtr}
form_type: {form_type}
predicate:
{predicate_code}
'''.format(**locals()))

i = 0
index = get_index(year, qtr, form_type, verbose=False)
for index_info in filter(predicate, index):
    i += 1
    print('Form for ' + index_info['Company Name'])
    name = index_info['Company Name'].replace('/', '_').replace('\\', '_')
    path = base_dir / name
    if form_type == '10-k':
        get_10k_items(index_info['Filename'], enable_cache=True, debug_path=path)
    else:
        get_8k_items(index_info['Filename'], enable_cache=True, debug_path=path)
