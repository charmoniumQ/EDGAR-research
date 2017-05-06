from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors


year = 2016
qtr = 3
type = '10-k'



def get_sections(year, qtr):
    for record in get_index(year, qtr, enable_cache=True, verbose=False, debug=True):
        rf = get_risk_factors(record['Filename'], enable_cache=True, verbose=False, debug=False, throw=False)
        path = 'results/risk-factors/' + type + '-' + str(year) + "-" + str(qtr) + "-" + str(record['CIK']) + ".txt"
        if str(rf) != 'None':
            with open(path, 'w+') as f:
                f.write(str(rf))
            f.close()



get_sections(2016, 3)
