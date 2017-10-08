import random
import toolz
import dask.bag
from edgar_code.util import new_directory
from edgar_code.cloud import KVBag
from edgar_code.retrieve import download_many_indexes, indexes_to_rfs, download_indexes


@toolz.curry
def pick_n(indexes, n):
    return KVBag(dask.bag.from_sequence(random.sample(indexes.compute(), n)))


def try_parse(years, form_type, filterer, debug_dir):
    #indexes = download_many_indexes(years, form_type=form_type) # mee
    indexes = download_indexes(years[0], 2, form_type) # mee
    indexes = filterer(indexes)
    rfs = indexes_to_rfs(indexes, debug_dir=debug_dir)
    for record, rf in rfs.compute():
        print(record)


if __name__ == '__main__':
    # years = list(range(2008, 2016))
    years = [2008]
    form_type = '10-K'
    filterer = pick_n(n=1)
    debug_dir = new_directory()
    print('results in', debug_dir)
    try_parse(years, form_type, filterer, debug_dir)
