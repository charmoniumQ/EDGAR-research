# flake8: ignore=E501
# pylint: disable C0103 C0111
import itertools
from enum import Enum
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_10k_items
from mining.parsing import ParseError
from util.timer import add_time
from cluster_config.spark import spark_cache, sc


class Status(Enum):
    ERROR = 1
    NOT_FOUND = 2
    SUCCESS = 3


@add_time
def download(index):
    '''
Outputs:
{
    'index': {
        see mining.retreive_index for how this works
    },
    'status': elem of Status enum,
    'form': {
        'item label': 'string item ontents
    }
}
'''
    output = dict(index=index.copy())
    try:
        form = get_10k_items(index['Filename'], enable_cache=False)
    except ParseError as e:
        output['status'] = Status.ERROR
        output['desc'] = str(e)
    else:
        output['status'] = Status.SUCCESS
        output['form'] = form
    return output


def filter_items(item):
    @add_time
    def filter_items_(record):
        output = record.copy()
        if output['status'] == Status.ERROR:
            return record
        else:
            if item not in record['form']:
                output['status'] = Status.NOT_FOUND
                output['keys'] = list(record['form'].keys())
            else:
                output['status'] = Status.SUCCESS
                output['item'] = record['form'][item]
            del output['form']
            return output
    return filter_items_


@spark_cache('gs://arcane-arbor-7359/', './transfer/')
def get_items(year, qtr, item):
    index = get_index(year, qtr, enable_cache=True)
    return (
        sc()
        .parallelize(index, 50)
        .map(download)
        .map(filter_items(item))
    )


def filter_status(status):
    def filter_status_(record):
        return record['status'] == status
    return filter_status_


def get_index_range(year_range, item):
    rdds = []
    for year in year_range:
        for qtr in range(1, 4):
            rdds.append(get_items(year, qtr, item))
    return sc().union(rdds)
