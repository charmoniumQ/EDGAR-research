from __future__ import print_function
from io import BytesIO
from zipfile import ZipFile
from re import sub
from itertools import islice
from datetime import datetime
import sys
import mining.cache as cache

# ftp://ftp.sec.gov/edgar/daily-index/2016/QTR3/
# ftp://ftp.sec.gov/edgar/full-index/2016/QTR3/
# ftp://ftp.sec.gov/edgar/monthly/

indexes = {'company', 'master', 'form'}

def download_index_uncompressed(year, qtr, index):
    '''Download the uncompressed .idx file. This takes a very long time.
    You are responsible for closing the file.'''
    # download the index file
    path = 'edgar/full-index/{year}/QTR{qtr}/{index}.idx'.format(**locals())
    return cache.download(path)

def download_index(year, qtr, index):
    '''Download the zip file and extract the .idx file from it.
    You are responsible for closing the file.'''
    compressed_path = 'edgar/full-index/{year}/QTR{qtr}/{index}.zip'.format(**locals())
    uncompressed_path = compressed_path + '_{index}.idx'.format(**locals())
    try:
        return cache.get(uncompressed_path)
    except cache.NotFound:
        compressed_file = cache.download_no_cache(compressed_path)

        # unzip the file
        with ZipFile(BytesIO(compressed_file), 'a') as index_zip:
            # extract {index}.idx where {index} gets replaced with
            # 'company', 'master', or 'form'
            uncompressed_file = index_zip.open('{index}.idx'.format(**locals()))
            if cache.ENABLE_CACHING:
                cache.put(uncompressed_path, uncompressed_file)
                uncompressed_file.close()
                return cache.get(uncompressed_path)
            else:
                return uncompressed_file

def normalize(line):
    '''Returns an list of elements found on each line (uppercased)'''
    line = line.decode() # turns binary string into ascii string
    line = line.strip() # removes trailing newline and leading spaces
    line = sub(' {2,}', '|', line) # 'a    b    c' -> 'a|b|c'
    elems = line.split('|') # 'a|b|c' -> ['a', 'b', 'c']
    while len(elems) > 5:
        elems[1] += ' ' + elems[2]
        del elems[2]
    return elems

types = {
    # name_of_type: funciton_which_converts_to_that_type
    'Form Type': str,
    'Company Name': str,
    'CIK': int,
    'Date Filed': lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
    'Filename': str,
}
aliases = {
    'File Name': 'Filename'
}

def parse_index(index_file):
    '''Reads the filename and parses it (provided the file came from download_index or download)'''
    # the headings occur in a different order based on what index you are
    # looking at (eg. FORM TYPE comes first in the form.idx)
    heading_line = next(islice(index_file, 8, 9)) # get the 8th line
    col_headings = normalize(heading_line)
    # map items through aliases first and then types
    col_headings = [word if word not in aliases else aliases[word] for word in col_headings]
    next(index_file) # skip the line with dashes
    for line in index_file:
       	elems = normalize(line)
       	# convert type of elem using the function associated with its column heading
        try:
            elems = {heading: types[heading](elem) for heading, elem in zip(col_headings, elems)}
        except Exception as e:
            print(elems)
            raise e
        else:
            yield elems

def get_index(year, qtr):
    '''Download the given index and cache it to disk.
If a cached copy is available, use that instead.
Caches are stored in data/ directory

    year: a string or int 4-digit year
    qtr: a string or int between 1 and 4
    See ftp://ftp.sec.gov/edgar/full-index/2016/QTR3/'''

    year = int(year)
    qtr = int(qtr)
    if not (1 <= qtr <= 4):
        raise ValueError('Quarter must be between 1 and 4')

    index_file = download_index(year, qtr, 'form')
    found_10ks = False
    for index_record in parse_index(index_file):
        if index_record['Form Type'] == '10-K':
            found_10ks = True
            yield index_record
        else:
            if found_10ks:
                break
    index_file.close()

if __name__ == '__main__':
    form_index = get_index(2016, 3)
    print('Press enter for another record')
    while input() == '':
        print(next(form_index))

__all__ = ['get_index']
