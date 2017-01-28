from cache import download
from itertools import islice
from retrieve_index import get_index
from retrieve_10k import SGML_to_files, get_risk_factors

form_index = get_index(2016, 3)
i = 0
with open('good.txt', 'w') as good, open('bad.txt', 'w') as bad:
    for index_info in form_index:
        path = index_info['Filename']
        i += 1
        try:
            get_risk_factors(path)
        except Exception as e:
            print(i, index_info['Company Name'], 'bad')
            print(index_info['Company Name'], file=bad)
        else:
            print(i, index_info['Company Name'], 'good')
            print(index_info['Company Name'], file=good)
