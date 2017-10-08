import sys
import csv
import random
import os.path
import urllib.request
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors
# from analysis.new_directory import new_directory, get_name
from util.new_directory import new_directory, get_name

year = 2016
qtr = 3

def is_paragraph(paragraph):
    return len(paragraph) > 20

def is_hacking(paragraph):
    paragraph = paragraph.lower()
    return 'information security' in paragraph

def urlify(record):
    _, _, cik, other = record['Filename'].split('/')
    doc, _ = other.split('.')
    doc_nodash = doc.replace('-', '')
    return 'https://www.sec.gov/Archives/edgar/data/{cik}/{doc_nodash}/{doc}-index.htm'.format(**locals())

def get_paragraphs_index(year, qtr, predicate):
    for record in get_index(year, qtr, enable_cache=True, verbose=False, debug=True):
        rf = get_risk_factors(record['Filename'], enable_cache=True, verbose=False, debug=False, throw=False)
        if rf:
            paragraphs = list(filter(is_paragraph, rf.split('\n')))
            i = 0
            for paragraph in filter(predicate, paragraphs):
                i += 1
                yield (record, paragraph)
            for paragraph in random.sample(paragraphs, i):
                yield (record, paragraph)
                
paragraphs = get_paragraphs_index(year, qtr, is_hacking)

directory = new_directory()
name = '_'.join(get_name())
fname = 'paragraphs_{name}.csv'.format(**locals())
fname = os.path.join(directory, fname)
print(fname)

with open(fname, 'w') as f:
    writer = csv.DictWrtier(f, fieldnames=['number', 'text', 'CIK', 'year', 'qtr', 'address'])
    writer.writeheader()
    for i, (record, paragraph) in enumerate(paragraphs):
        writer.writerow({
            'number': i,
            'paragraph': paragraph,
            'CIK': record['CIK'],
            'year': year,
            'qtr': qtr,
            'address': urlify(record),
        })
        f.flush()
        print(i)
