from __future__ import print_function
import re
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors
from util.new_directory import new_directory
from util.stem import stem
import itertools

'''
This script downloads and parses risk factors
year: (int)
qtr: (int) 1-4
'''

year = 2016
qtr = 3

directory = new_directory()
print('Results in ' + directory.as_posix())

# write meta-data
with (directory / 'info.txt').open('w+') as f:
    f.write('''script: {__file__}
year: {year}
qtr: {qtr}
'''.format(**locals()))

for record in get_index(year, qtr, enable_cache=True):
# for record in itertools.islice(get_index(year, qtr, enable_cache=True), None, 100):
    name = record['Company Name']
    # TODO: lint
    # TODO: move this to new_directory
    import re
    name = re.sub('[^a-zA-Z0-9]', '_', name)
    file_name = '{name}.txt'.format(**locals())
    rf = get_risk_factors(record['Filename'], enable_cache=True, throw=False)
    if len(rf) > 1000:
        print(record['Company Name'])
        with (directory / file_name).open('w+', encoding='utf-8') as f:
            f.write(stem(rf))
