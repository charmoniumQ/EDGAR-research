from __future__ import print_function
import os
from mining.cache import download
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, extract_to_disk, parse_10k
from analysis.new_directory import new_directory

i = 0

def predicate(company_name, CIK):
    # take = CIK == 9238 or company_name == 'blah'
    global i; i += 1
    return i < 20

directory = new_directory()
for index_info in get_index(2016, 3, enable_cache=True, verbose=False, debug=True):
    company_name = index_info['Company Name'].upper()
    dirname = directory + company_name.replace('/', '_').replace('\\', '_') + '/'
    CIK = index_info['CIK']
    if predicate(company_name, CIK):
        raw_file = download(index_info['Filename'], enable_cache=True, verbose=False, debug=True)
        files = SGML_to_files(raw_file, verbose=True, debug=True)
        while os.path.exists(dirname):
            dirname += '_'
        extract_to_disk(dirname, files, verbose=True, debug=True)
        tempdir = dirname + '/temp/'
        os.mkdir(tempdir)
        try:
            parse_10k(files, verbose=True, debug=True, path=tempdir)
        except:
            import traceback; traceback.print_exc()
