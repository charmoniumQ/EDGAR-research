from __future__ import print_function
import re
import datetime
import mining.cache as cache
import itertools

def get_index(year, qtr, form_type="10-k", enable_cache=True, verbose=False):
    '''Download the given index and return entries from it

    year: a 4-digit int year
    qtr: an int between 1 and 4
    form_type: '10-k' or '8-k'
    enable_cache: if True, the index will be stored on the disk
    verbose: if True, print when downloading

yields a dict with keys "Company Name" (str), "CIK" (int), "Date Filed" (date), "Filename" (str)'''

    if not (1 <= qtr <= 4):
        raise ValueError('Quarter must be between 1 and 4')

    index_file = download_index(year, qtr, 'form', enable_cache, verbose)
    found_section = False
    for index_record in parse_index(index_file):
        if index_record['Form Type'] == form_type:
            found_section = True
            del index_record['Form Type']
            yield index_record
        else:
            if found_section:
                break

def download_index(year, qtr, index, enable_cache, verbose):
    '''Download the uncompressed .idx file.'''
    path = 'edgar/full-index/{year}/QTR{qtr}/{index}.idx'.format(**locals())
    return cache.download(path, enable_cache, verbose)

def normalize(line):
    '''Returns an list of elements found on each line (uppercased)'''
    line = line.decode() # turns binary string into ascii string
    line = line.strip() # removes trailing newline and leading spaces
    line = re.sub(' {2,}', '|', line) # 'a    b    c' -> 'a|b|c'
    elems = line.split('|') # 'a|b|c' -> ['a', 'b', 'c']
    while len(elems) > 5:
        elems[1] += ' ' + elems[2]
        del elems[2]
    if len(elems) < 5:
        elems.insert(1, '')
    return elems

def translate(plain, table):
    '''If a word appears as a key in table, replace it with table[word]. Otherwise leave the word unchanged'''
    return [table[word] if word in table else word
            for word in plain]

types = {
    # name_of_type: function_which_converts_to_that_type
    'Form Type': lambda x: str(x).lower(),
    'Company Name': lambda x: str(x).upper(),
    'CIK': int,
    'Date Filed': lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(),
    'Filename': str,
}
aliases = {
    'File Name': 'Filename'
}

def iindex(iterator, i):
    '''Gets the ith element of the iterator, forwarding the sequence to that spot'''
    return next(itertools.islice(iterator, i, i + 1))

def parse_index(index_file):
    '''Reads the filename and parses it (provided the file came from download_index or download)'''

    # see https://www.sec.gov/Archives/edgar/full-index/2016/QTR3/form.idx

    # the headings occur in a different order based on what index you are
    # looking at (eg. FORM TYPE comes first in the form.idx)
    index_file = iter(index_file.split(b'\n'))

    # get the 8th line, the previous 7 are useless information
    heading_line = iindex(index_file, 8)
    col_headings = translate(normalize(heading_line), aliases)

    # skip the line with dashes
    next(index_file) 
    for line in index_file:
       	elems_list = normalize(line)
       	# convert type of elem using the function associated with its column heading
        try:
            elems_dict = {}
            for heading, elem in zip(col_headings, elems_list):
                elems_dict[heading] = types[heading](elem)
        except Exception as e:
            print(elems_list)
            raise e
        else:
            yield elems_dict

__all__ = ['get_index']
