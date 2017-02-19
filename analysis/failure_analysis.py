from __future__ import print_function
from itertools import islice
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors
from analysis.new_directory import new_directory

directory = new_directory()
i = 0
with open(directory + 'good.txt', 'w') as good, open(directory + 'bad.txt', 'w') as bad:
    for index_info in get_index(2016, 3, enable_cache=True, verbose=False, debug=True):
        path = index_info['Filename']
        i += 1
        try:
            get_risk_factors(path, enable_cache=True, verbose=False, debug=True, wpath=directory)
        except Exception as e:
            print(i, index_info['Company Name'], 'bad')
            print(index_info['Company Name'], file=bad)
        else:
            print(i, index_info['Company Name'], 'good')
            print(index_info['Company Name'], file=good)
