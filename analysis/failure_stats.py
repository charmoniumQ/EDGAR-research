from __future__ import print_function
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_10k_items
from mining.retrieve_8k import get_8k_items
from util.new_directory import new_directory
import datetime
import sys
import traceback

'''
This script computes the failure/success rate of form parsing.
It also prints the time taken to parse the documents, and the size of the
selected item in each document.

year (int)
qtr (int): must be 1 through 4
form_type: lowercase string (must be '10-k' and '8-k')
stop_at: (int or None)
  if stop_at is an int, it computes the failure rate among the first `stop_at`
  documents.
  If stop_at is None, it computes the failure rate for all of the documents in
  that quarter.
item: (must be '1a' or '5.02' or something similar) the item to select from each
  form. This script will measure the size of that item.
'''
form_type = '10-k'
year = 2005
qtr = 1
stop_at = 5
item = '1a'

# select function for getting form
if form_type == '10-k':
    get_document = get_10k_items
    kwargs = dict(enable_cache=True)
elif form_type == '8-k':
    get_document = get_8k_items
    kwargs = dict(enable_cache=True)
else:
    raise RuntimeError('Cannot handle form type: ' + form_type)

directory = new_directory()
print('Results in ' + directory.as_posix())

# Write info text file
with (directory / 'info.txt').open('w') as info:
    info.write('''script: {__file__}
form_type: {form_type}
year: {year}
qtr: {qtr}
stop_at: {stop_at}
item: {item}
'''.format(**locals()))

good_count = 0
with_item_count = 0
bad_count = 0
char_count = 0

start = datetime.datetime.now()
with (directory / 'good.txt').open('w') as good, \
     (directory / 'bad.txt').open('w') as bad, \
     (directory / 'errors.txt').open('w') as error:
    files = [good, bad, error, sys.stdout]

    for i, index_info in enumerate(get_index(year, qtr, form_type=form_type)):
        if stop_at is not None and i >= stop_at:
            break
        else:
            path = index_info['Filename']
            name = index_info['Company Name']

            try:
                items = get_document(path, **kwargs)
            except Exception as e:
                bad_count += 1
                print(i, name, 'bad')
                print(name, file=error)
                traceback.print_exc(file=error)
                print(name, file=bad)
            else:
                good_count += 1
                print(i, name, 'good', end='')
                print(name, file=good)
                if item in items:
                    with_item_count += 1
                    char_count += len(items[item])
                    print()
                else:
                    print(' '.join(sorted(items.keys())))
        # flushing the files writes the temporary output to each file
        [file.flush() for file in files]
stop = datetime.datetime.now()

# compute results
total_count = good_count + bad_count
hit_rate = good_count / total_count * 100
time = (stop - start).total_seconds()
avg_time = time / total_count
size = char_count / 1024 / 1024
item_hit_rate = with_item_count / total_count * 100
if with_item_count != 0:
    avg_size = char_count / with_item_count / 1024
else:
    avg_size = 0

# print results (to screen and to file)
res = '''hit rate: {hit_rate:.0f}% ({good_count} / {total_count})
time: {time:.2f}s (average of {avg_time:.2f}s)
item hit rate: {item_hit_rate:.0f}% ({with_item_count} / {total_count})
size of item: {size:.0f} mb (average of {avg_size:.0f} kb)
'''.format(**locals())
print(res)
with (directory / 'info.txt').open('a') as info:
    info.write(res)
