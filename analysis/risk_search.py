from __future__ import print_function
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors
from analysis.new_directory import new_directory
import os

# Change this
def risk_predicate(risk_factors):
    risk_factors = risk_factors.lower()
    # take = ('hacking' in risk_factors and 'spinach' not in risk factors) or 'lettuce' in risk_factors
    take = True
    start = 0
    stop = -1
    return take, start, stop

# uppercase list of filters
# if no companies are listed, no filter is applied
# Change this
companies = ['ADM TRONICS UNLIMITED, INC.']

directory = new_directory()

for index_info in get_index(2016, 3):
    company_name = index_info['Company Name'].upper()
    path = index_info['Filename']

    if not companies or company_name in companies:
        print(company_name)
        try:
            risk_factors = get_risk_factors(path, debug=True)
        except Exception as e:
            pass
        else:
            take, start, stop = risk_predicate(risk_factors)
            if take:
                print('Y', index_info['Company Name'])
                fname = os.path.join(directory, index_info['Company Name'] + '.txt')
                with open(fname, 'w', encoding='utf-8') as file:
                    file.write('CIK = ' + str(index_info['CIK']))
                    file.write(risk_factors[start:stop])
            else:
                # print('N', index_info['Company Name'])
                pass
