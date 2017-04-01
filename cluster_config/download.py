import itertools
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_items, ParseError
from cluster_config import timer, init_spark

sc = init_spark.get_sc('download')

def download_risk(filing_info):
    output = filing_info.copy()
    with timer.timer():
        try:
            items = get_items(filing_info['Filename'], debug=False, enable_cache=False, verbose=False)
        except ParseError as e:
            output['error'] = True
        else:
            output['error'] = False
            output.update(items)
    output['download_time'] = timer.time
    return output

year = 2016
qtr = 1
docs = 1000
folds = 80
output = 'gs://output-bitter-voice/10k_data-{year}-qtr{qtr}-{docs}'.format(**locals())
with timer.timer(print_=True):
    index = get_index(year, qtr, enable_cache=False, verbose=False, debug=False)
    risk_data = sc.parallelize(itertools.islice(index, docs), folds).map(download_risk)
    risk_data.saveAsPickleFile(output)
