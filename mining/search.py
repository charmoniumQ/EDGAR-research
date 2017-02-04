from __future__ import print_function
import os
import os.path
from retrieve_index import get_index
from retrieve_10k import SGML_to_files, get_risk_factors

# Change this
def predicate(risk_factors):
    risk_factors = risk_factors.lower()
    # take = ('hacking' in risk_factors and 'spinach' not in risk factors) or 'lettuce' in risk_factors
    take = 'information security' in risk_factors
    return take


directory = os.path.join('results', 'r')
i = 0
while os.path.isdir(directory + str(i)):
    i += 1
directory += str(i)
os.mkdir(directory)


form_index = get_index(2016, 3)
while True:
    index_info = next(form_index)
    path = index_info['Filename']
    try:
        risk_factors = get_risk_factors(path, debug=False)
    except Exception as e:
        pass
    else:
        if predicate(risk_factors):
            print('Y', index_info['Company Name'])
            fname = os.path.join(directory, index_info['Company Name'] + '.txt')
            with open(fname, 'w', encoding='utf-8') as file:
                file.write(index_info['CIK'])
                file.write(risk_factors)
        else:
            print('N', index_info['Company Name'])
