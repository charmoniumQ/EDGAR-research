from __future__ import print_function
from mining.cache import download
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors, extract_to_disk
from analysis.new_directory import new_directory

def predicate(company_name, CIK):
    # take = CIK == 9238 or company_name == 'blah'
    take = company_name == '1ST SOURCE CORP'
    return take

directory = new_directory(make=False)
for index_info in get_index(2017, 1):
    company_name = index_info['Company Name'].upper()
    CIK = index_info['CIK']
    if predicate(company_name, CIK):
        raw_file = download(index_info['Filename'])
        files = SGML_to_files(raw_file)
        extract_to_disk(directory, files)
