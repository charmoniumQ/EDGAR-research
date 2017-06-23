# flake8: ignore=E501
# pylint: disable C0103 C0111
from enum import Enum
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_10k_items
from mining.parsing import ParseError
from util.timer import insert_time, add_time
from cluster_config.init_spark import init_spark
from cluster_config.init_spark import spark_cache


class Status(Enum):
    ERROR = 1
    NOT_FOUND = 2
    SUCCESS = 3


@add_time
def downlaod(record):
    output = record.copy()
    try:
        form = get_10k_items(record['Filename'], enable_cache=False)
    except ParseError as e:
        output['status'] = Status.ERROR
        output['desc'] = str(e)
    else:
        output['status'] = Status.SUCCESS
        output['form'] = form
    return output


@add_time
def filter_items(item):
    def filter_items_(record):
        output = record.copy()
        if output['status'] == Status.ERROR:
            return record
        else:
            if item not in output['form']:
                output['error'] = Status.NOT_FOUND
                output['keys'] = output['form'].keys()
                del output['form']
            else:
                output['error'] = Status.SUCCESS
                output['item'] = output['form'][item]
                del output['form']
    return filter_items_


@spark_cache
def get_items(year, qtr, item):
    return (
        sc.parallelize(get_index(year, qtr, enable_cache=False))
        .map(download)
        .map(filter_items(item))
    )


def get_index_range(year_range, item):
    rdds = []
    for year in year_range:
        for qtr in range(1, 4):
            rdds.append(get_items(year, qtr, item))
    return sc.union(rdds)


if __name__ == '__main__':
    global sc; sc = init_spark.get_sc('download')
    year_range = range(2007, 2017)
    counts = (
        get_index_range(year_range, '1a')
        .map(count)
        .reduce(dict_add)
    )


# year = 2016
# qtr = 1
# docs = 1000
# folds = 80
# output = 'gs://output-bitter-voice/10k_data-{year}-qtr{qtr}-{docs}'.format(**locals())
# with timer.timer(print_=True):
#     index = get_index(year, qtr,enable_cache=False, verbose=False, debug=False)
#     risk_data = sc.parallelize(itertools.islice(index, docs), folds).map(download_risk)
#     risk_data.saveAsPickleFile(output)
