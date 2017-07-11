import random
import inspect
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_10k_items
from mining.retrieve_8k import get_8k_items
from util.paragraphs import to_paragraphs, group_paragraphs, \
    p_paragraphs, p_groups
from util.new_directory import new_directory

'''
This script downloads and parses forms.
It writes each company into its own folder.
It writes all of the intermediate steps as well.

year: (int)
qtr: (int) from 1-4
form_type: (lowercase string) Currently only '10-k' and '8-k' are supported
predicate: a function that consumes an index_info and returns a boolean
item: (lowercase string) If present, it prints the paragraphs in this item
randomly_select: (boolean) if true, then the 10-ks are randomly selected (still
filtered by predicate)
random_n: (int) if randomly_select is True, select this many
enable_cache: (bool)
'''


year = 2006
qtr = 3
form_type = '10-k'
item = '1a'
enable_cache = False
randomly_select = True
random_n = 2

def predicate(index_info):
    # Uncomment one of the following lines
    # return index_info['Company Name'] == 'AMERICAN SOFTWARE INC'
    # return index_info['CIK'] == 1002010
    return True  # takes all


# TODO: add risk predicate (delete risk_search)
# TODO: add option to only write a specific item
# TODO: refactor metadata writer into new_directory

base_dir = new_directory()
print('Results in ' + base_dir.as_posix())

# Write info file
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
index = get_index(year, qtr, form_type, verbose=False, enable_cache=enable_cache) # noqa
if randomly_select:
    index = random.sample(list(index), random_n)
for index_info in filter(predicate, index):
    i += 1
    print('Form for ' + index_info['Company Name'])
    name = index_info['Company Name'].replace('/', '_').replace('\\', '_')
    path = base_dir / name
    if form_type == '10-k':
        doc = get_10k_items(index_info['Filename'], enable_cache=enable_cache, debug_path=path) # noqa
    else:
        doc = get_8k_items(index_info['Filename'], enable_cache=enable_cache, debug_path=path) # noqa
    if item:
        if item in doc:
            text = doc[item]
            with (path / 'item_text').open('w+') as f:
                f.write(text)
            paragraphs = to_paragraphs(text)
            with (path / 'item_paragraphs').open('w+') as f:
                p_paragraphs(paragraphs, f)
            groups = group_paragraphs(paragraphs)
            with (path / 'item_groups').open('w+') as f:
                p_groups(groups, f)
        else:
            print('{item} not in {}'.format(doc.keys(), **globals()))
