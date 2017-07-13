# import re
import dask.bag
from . import retrieve
#from . import util


# @util.cache_bag
def download(year, qtr, form_type, item):
    '''Downloads form_type for year/qtr and saves the item

    form_type (str): "10-K" or "8-K"

    qtrs (int): financial quarter, 1 - 4
    year (int): eg. 2006
    item (str): element of form, eg. "Item 1A"
    '''

    index = retrieve.index(year, qtr, form_type)
    return (
        dask.bag.from_sequence(index)
        .map(download_record(form_type, item))
        .filter(bool)
    )


def download_record(form_type, item, *args, **kwargs):
    retrieve_form = retrievers[form_type]

    def download_record_(record):
        form = retrieve_form(record, *args, **kwargs)
        if item in form:
            return record, form[item]
    return download_record_


retrievers = {
    '10-K': retrieve._10K,
    '8-K': retrieve._8K,
}


if __name__ == '__main__':
    for record, rf in download(2014, 1, '10-K', 'Item 1A').take(10):
        print(rf[:100])
