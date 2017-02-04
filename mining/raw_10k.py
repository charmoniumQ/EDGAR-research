from __future__ import print_function
import os
import os.path
from cache import download
from retrieve_index import get_index
from retrieve_10k import SGML_to_files, get_risk_factors, extract_to_disk

directory = os.path.join('results', 'p')
i = 0
while os.path.isdir(directory + str(i)) or os.path.isfile(directory + str(i)):
    print(directory + str(i), 'exists')
    i += 1
directory += str(i)
print(directory, 'does not exists', os.path.isdir(directory))

def predicate(index_info):
    # take = index_info['CIK'] == 9238 or index_info['Company Name'].lower() == 'blah'
    take = index_info['Company Name'].lower() == '1 800 flowers com inc'
    return take

form_index = get_index(2016, 3)
for index_info in filter(predicate, form_index):
    raw_file = download(index_info['Filename'])
    files = SGML_to_files(raw_file.read())
    raw_file.close()
    extract_to_disk(directory, files)
