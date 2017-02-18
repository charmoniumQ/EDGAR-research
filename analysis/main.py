from __future__ import print_function
from itertools import islice
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors
from six.moves import input

form_index = get_index(2016, 3)
print("Press enter for another risk factor. Press 'q' to quit.")

while input() == '':
    index_info = next(form_index)
    path = index_info['Filename']
    print('Risk factors for ' + index_info['Company Name'])
    print('-'*70)
    try:
        print(get_risk_factors(path)[:1000])
    except Exception as e:
        print('Unable to get')
        print(path)
        print(e)
    print('\n')
