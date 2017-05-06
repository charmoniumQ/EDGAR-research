from __future__ import print_function
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors
from analysis.new_directory import new_directory
from mining.retrieve_8k import *

directory = new_directory()

type = '8-k'
document = get_departure_of_directors_or_certain_officers
kwargs = dict(enable_cache=True, verbose=False, debug=False, wpath=directory)
year = 2016
quarter = 3

i = 0
with open(directory + 'good.txt', 'w') as good, open(directory + 'bad.txt', 'w') as bad:
    for index_info in get_index(year, quarter, enable_cache=True, verbose=False, debug=True, type=type):
        path = index_info['Filename']
        i += 1
        try:
            document(path, **kwargs)
        except Exception as e:
            print(i, index_info['Company Name'], 'bad')
            print(index_info['Company Name'], file=bad)
        else:
            print(i, index_info['Company Name'], 'good')
            print(index_info['Company Name'], file=good)
