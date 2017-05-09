from __future__ import print_function
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors
from analysis.new_directory import new_directory
import csv
import os
import re

year = 2016
qtr = 1

# Company name filter: only search the following company names
# Company names must be upercase and surrouned by quotes and separated by comma
# if no companies are listed, no filter is applied
# companies = ['COMPANY A', 'COMPANY B']
companies = []

# Change this
def risk_predicate(risk_factors):
    risk_factors = risk_factors.lower()
    if re.search('climate change', risk_factors):
        # \n is a newline
        # .*? takes as FEW characters as possible, but as many as are needed
        take_text = []
        for hit in  re.finditer(".*?\n.*?\n.*?climate change.*?\n.*?\n.*?", risk_factors):
            take_text.append(hit.group(0))
        return True, ('\n' + '='*79).join(take_text)
    else:
        return False, ''

directory = new_directory()
contents_name = os.path.join(directory, 'contents.csv')
with open(contents_name) as contents_file:
    contents = csv.DictWriter(contents_file, ['status', 'CIK', 'Date Filed', 'Company Name'])
    for index_info in get_index(year, qtr, enable_cache=True, verbose=False, debug=True):
        company_name = index_info['Company Name']
        path = index_info['Filename']

        if not companies or company_name in companies:
            try:
                risk_factors = get_risk_factors(path, enable_cache=True, verbose=False, debug=True, wpath=directory)
            except Exception as e:
                print('Unable to get risk factors')
                print(e)
                import traceback; traceback.print_exc()
            else:
                take, text = risk_predicate(risk_factors)
                if take:
                    index_info['status'] = 'Y'
                    print('Y', index_info['Company Name'])
                    basename = index_info['Company Name'].replace('/', '_').replace('\\', '_')
                    fname = os.path.join(directory, basename + '.txt')

                    with open(fname, 'w', encoding='utf-8') as file:
                        file.write(text)
                else:
                    index_info['status'] = 'N'
                    print('N', index_info['Company Name'])
                contents.writerow(index_info)
