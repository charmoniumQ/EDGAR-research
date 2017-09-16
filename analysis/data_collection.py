from __future__ import print_function
import re
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors
from util.new_directory import new_directory
from util.stem import stem
import itertools

'''
This script downloads and parses risk factors
year: (int)
qtr: (int) 1-4
'''

year = 2017
qtr = 2

directory = new_directory()
print('Results in ' + directory.as_posix())


def is_paragraph(paragraph):
    return len(paragraph) > 20

# write meta-data
with (directory / 'info.txt').open('w+') as f:
    f.write('''script: {__file__}
year: {year}
qtr: {qtr}
'''.format(**locals()))


'''
Make database with Filing_Date, Year, Qtr, Company ID, hasRiskFactors
'''
with (directory/"rf_report.csv").open('w+', encoding='utf-8') as rf_report:
    for year in range(2006, 2018):
        for qtr in range(1, 5):
            print('YEAR', year, 'QTR', qtr)
            for record in get_index(year, qtr, enable_cache=False):
            # for record in itertools.islice(get_index(year, qtr, enable_cache=True), None, 100):
                name = record['Company Name']
                # TODO: lint
                # TODO: move this to new_directory
                import re
                name = re.sub('[^a-zA-Z0-9]', '_', name)
                file_name = '{name}.txt'.format(**locals())
                rf = get_risk_factors(record['Filename'], enable_cache=False, throw=False)
                valid_rf = len(rf) > 1000
                paragraphs = list(filter(is_paragraph, rf.split('\n')))
                # print(paragraphs)
                line = ','.join([str(x) for x in [record['Date Filed'], year, qtr, record['CIK'], name,
                                                  valid_rf, len(paragraphs)]])
                rf_report.write(line + '\n')
                print(line)

            rf_report.flush()
