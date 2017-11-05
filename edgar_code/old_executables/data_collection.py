from __future__ import print_function
import re
from ..util.new_directory import new_directory
from ..retrieve.main import download_all
import sys

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
if len(sys.argv) == 3:
	y1 = (int)(sys.argv[1])
	y2 = (int)(sys.argv[2])
else:
	y1 = 2009
	y2 = 2017
header = 'date_filed,year,qtr,cik,name,has_valid_rf, number_of_rf'
with (directory/"rf_report.csv").open('w+', encoding='utf-8') as rf_report:
    rf_report.write(header + '\n')
    for year in range(y1, y2):
        for qtr in range(1, 5):
            print('YEAR', year, 'QTR', qtr)
            for record, rf in download_all(year, qtr, '10-K', 'Item 1A').collect():
                name = record['Company Name']
                # TODO: lint
                # TODO: move this to new_directory
                name = re.sub('[^a-zA-Z0-9]', '_', name)
                file_name = '{name}.txt'.format(**locals())
                if rf is None:
                    valid_rf = False
                else:
                    paragraphs = list(filter(is_paragraph, rf.split('\n')))
                line = ','.join([str(x) for x in [record['Date Filed'], year,
                                                  qtr, record['CIK'], name,
                                                  valid_rf, len(paragraphs)]])
                rf_report.write(line + '\n')
                # print(line)

            rf_report.flush()
