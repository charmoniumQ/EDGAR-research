from cache import download
from itertools import islice
from retrieve_index import get_index
from retrieve_10k import SGML_to_files, get_risk_factors

form_index = get_index(2016, 3)
print("Press enter for another risk factor. Press 'q' to quit.")
#list(islice(form_index, 6))
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
