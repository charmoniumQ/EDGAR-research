from __future__ import print_function
import re
import datetime
import collections
import urllib.request
import zipfile
import io
import itertools
import operator


Record = collections.namedtuple('Record', [
    'form_type', 'company_name', 'CIK', 'date_filed', 'path', 'year', 'qtr'
])


def index(year, qtr, form_type):
    lines = download(year, qtr)
    col_names = parse_header(lines)
    records = parse_body(year, qtr, lines, col_names)
    return filter_form_type(records, form_type)


def download(year, qtr):
    index = 'form'
    url = 'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/form.zip'.format(**locals()) # noqa
    compressed_file = urllib.request.urlopen(url).read()
    compressed_file = io.BytesIO(compressed_file)

    # unzip the file
    with zipfile.ZipFile(compressed_file, 'r') as index_zip:
        uncompressed_file = index_zip.open('{index}.idx'.format(**locals()))
        return uncompressed_file


def parse_header(lines):
    # see https://www.sec.gov/Archives/edgar/full-index/2016/QTR3/form.idx

    # the headings occur in a different order based on what index you are
    # looking at (eg. FORM TYPE comes first in the form.idx)

    # throw away the first 8 lines (just informational)
    for _ in range(8):
        next(lines)

    col_names = split(next(lines))

    # sometimes 'File Name' is two words
    if 'File Name' in col_names:
        # replace it with 'Filename'
        col_names[col_names.index('File Name')] = 'Filename'

    next(lines)  # throw away next line
    return col_names


def parse_body(year, qtr, lines, col_names):
    for line in lines:
        # elems_dict is a dict from col_heading (eg. 'CIK')
        # to value (eg. 1020391)
        # becauset the order is not guarunteed
        line_dict = dict(zip(col_names, split(line)))

        out = {
            'year': year,
            'qtr': qtr,
            'form_type': line_dict['Form Type'].lower(),
            'company_name': line_dict['Company Name'].upper(),
            'CIK': int(line_dict['CIK']),
            'date_filed': datetime.datetime.strptime(
                line_dict['Date Filed'], '%Y-%m-%d').date(),
            'path': 'https://www.sec.gov/Archives/' + line_dict['Filename'],
        }
        yield Record(**out)


def filter_form_type(records, this_form_type):
    form_type_records = itertools.groupby(records, operator.attrgetter('form_type'))
    for form_type, records in form_type_records:
        if this_form_type == form_type:
            yield from records
    else:
        return []


def split(line):
    '''Returns an list of elements found on each line'''

    # turns line into a string
    line = line.decode()

    # removes trailing newline and leading spaces
    line = line.strip()

    # 'a    b    c' -> 'a|b|c'
    # some indexes are separated by spaces, others are separted by |
    # this normalizes them all to be |-separated
    line = re.sub(' {2,}', '|', line)

    # 'a|b|c' -> ['a', 'b', 'c']
    elems = line.split('|')

    # too many elements, elems[2] should be part of elems[1]
    while len(elems) > 5:
        elems[1] += ' ' + elems[2]
        del elems[2]

    # too few elements, empty field present
    if len(elems) < 5:
        elems.insert(1, '')

    return elems


__all__ = ['index']
