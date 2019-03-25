import random
import toolz
import dask.bag
from edgar_code.util import new_directory, sanitize_unused_fname
from edgar_code.cloud import KVBag
from edgar_code.retrieve import index_to_rf, download_indexes
from dask.diagnostics import ProgressBar


def main(year, qtr, filterer, dir_):
    # compute which indexes to pull locally
    indexes = download_indexes('10-K', year, qtr)
    indexes = filterer(indexes)

    # cluster computing part
    rfs = KVBag.from_keys(indexes).map_values(index_to_rf)

    # collect and store locally
    pbar = ProgressBar()
    pbar.register()
    for record, rf in rfs.compute():
        fname = sanitize_unused_fname(dir_, record.company_name, 'txt')
        if rf:
            print(f'{record.company_name} -> {fname.name}')
            with fname.open('w') as f:
              f.write(rf)
        else:
            print(f'No risk factor for {record.company_name}')


@toolz.curry
def pick_n(indexes, n):
    return dask.bag.from_sequence(random.sample(indexes.compute(), n))


if __name__ == '__main__':
    year = 2008
    qtr = 2
    filterer = pick_n(n=2)
    dir_ = new_directory()
    main(year, qtr, filterer, dir_)
