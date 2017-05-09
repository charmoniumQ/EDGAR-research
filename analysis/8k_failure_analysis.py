from __future__ import print_function
from itertools import islice
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files
from mining.retrieve_8k import get_departure_of_directors_or_certain_officers
from analysis.new_directory import new_directory

directory = new_directory()
i = 0
with open(directory + 'good.txt', 'w') as good, open(directory + 'bad.txt', 'w') as bad:
    for index_info in get_index(2016, 3, enable_cache=True, verbose=False, debug=False, type="8-k"):
        path = index_info['Filename']
        i += 1
        try:
            t = get_departure_of_directors_or_certain_officers(path, enable_cache=True, verbose=False, debug=False, wpath=directory)
        except Exception as e:
            print(i, index_info['Company Name'], 'bad')
            print(index_info['Company Name'], file=bad)
        else:
            print(i, index_info['Company Name'], 'good')
            print(index_info['Company Name'], file=good)
            print(t)
