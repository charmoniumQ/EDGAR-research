from __future__ import print_function
from itertools import islice
from mining.retrieve_index import get_index
from mining.retrieve_10k import SGML_to_files, get_risk_factors


size = 0
good = 0
bad = 0
total = 0

year = 2013
qtr = 1

form_index = get_index(year, qtr)
print("Press enter for another risk factor. Press 'q' to quit.")
#list(islice(form_index, 6))
with open('results/{year}_{qtr}_10k_char_count.txt'.format(**locals()), 'w') as char_count_file, open('results/{year}_{qtr}_10k_stats.text'.format(**locals()), 'w') as stats_file:
    for index_info in form_index:
        if good%10 == 0:
            print("SUB_TOTAL=", total)

        path = index_info['Filename']
        print('Risk factors for ' + index_info['Company Name'])
        print('-'*70)
        try:
            risk_factor = get_risk_factors(path)
            size = len(risk_factor)
            print(size)
            print(size, file=char_count_file)
            total = total + size
            good = good + 1


        except Exception as e:
            print('Unable to get')
            print(path)
            print(e)
            bad = bad + 1
        print('\n')

    print("good: ", good, file=stats_file)
    print("bad: ", bad, file=stats_file)
    print("total_char_count: ", total, file=stats_file)
    print("EOF")
