from __future__ import print_function
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_10k_items
from mining.retrieve_8k import get_8k_items
from util.new_directory import new_directory
import datetime
import sys
import traceback

form_type = '10-k'
year = 2016
qtr = 3
stop_at = 100

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
with (directory / 'info.txt').open('w') as info:
    info.write('''
form_type: {form_type}
year: {year}
qtr: {qtr}
'''.format(**locals()))

good_count = 0
bad_count = 0
start = datetime.datetime.now()
with (directory / 'good.txt').open('w') as good, \
     (directory / 'bad.txt').open('w') as bad, \
     (directory / 'errors.txt').open('w') as error:
    files = [good, bad, error, sys.stdout]
    for i, index_info in enumerate(get_index(year, qtr, form_type=form_type)):
        if i >= stop_at:
            break
        else:
            path = index_info['Filename']
            name = index_info['Company Name']

            try:
                get_document(path, **kwargs)
            except Exception as e:
                bad_count += 1
                print(i, name, 'bad')
                error.write(name + '\n')
                traceback.print_exc(file=error)
                bad.write(name + '\n')
                [file.flush() for file in files]
            else:
                good_count += 1
                print(i, name, 'good')
                good.write(name + '\n')
                [file.flush() for file in files]
stop = datetime.datetime.now()

with (directory / 'info.txt').open('a') as info:
    total_count = good_count + bad_count
    avg = good_count / total_count * 100
    elapsed = (stop - start).total_seconds()
    res = '''
avg: {avg:.01f}% ({good_count} / {total_count})
time: {elapsed}
'''.format(**locals())
    info.write(res)
    print(res)
