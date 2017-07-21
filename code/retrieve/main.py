import dask.bag
from . import index
from . import _8K
from . import _10K


# @util.cache_bag
def download_all(year, qtr, form_type, item):
    '''Downloads form_type for year/qtr and saves the item

    form_type (str): "10-K" or "8-K"

    qtrs (int): financial quarter, 1 - 4
    year (int): eg. 2006
    item (str): element of form, eg. "Item 1A"
    '''

    index_ = index.download(year, qtr, form_type)
    return (
        dask.bag.from_sequence(index_)
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
    '10-K': _10K.download,
    '8-K': _8K.download,
}


if __name__ == '__main__':
    for record, rf in download_all(2014, 1, '10-K', 'Item 1A').take(10):
        print(rf[:100])
