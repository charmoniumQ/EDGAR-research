import random
import toolz
import dask.bag
from edgar_code.util import new_directory, sanitize_fname, unused_fname
from edgar_code.cloud import KVBag
from edgar_code.retrieve import index_to_rf, download_indexes


@toolz.curry
def pick_n(indexes, n):
    return KVBag(dask.bag.from_sequence(random.sample(indexes.compute(), n)))


def main(year, qtr, filterer, dir_):
    indexes = download_indexes('10-K', year, qtr)
    indexes = filterer(indexes)
    rfs = indexes.map_values(index_to_rf)
    for record, rf in rfs.compute():
        starting_fname = sanitize_fname(record.company_name)
        fname = unused_fname(dir_, starting_fname).with_suffix('.txt')
        print('{record.company_name} -> {fname.name}'.format(**locals()))
        with fname.open('w') as f:
            f.write(rf)


if __name__ == '__main__':
    year = 2008
    qtr = 2
    filterer = pick_n(n=1)
    dir_ = new_directory()
    print('results in', dir_)
    main(year, qtr, filterer, dir_)
