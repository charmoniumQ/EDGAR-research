import random
import toolz
import dask.bag
from edgar_code.util import new_directory, sanitize_fname, unused_fname
from edgar_code.cloud import KVBag
from edgar_code.retrieve import index_to_directors, download_indexes


@toolz.curry
def pick_n(indexes, n):
    return KVBag(dask.bag.from_sequence(random.sample(indexes.compute(), n)))


def main(year, qtr, filterer, dir_):
    indexes = download_indexes('8-K', year, qtr)
    indexes = filterer(indexes)
    directors = indexes.map_values(index_to_directors)
    for record, director in directors.compute():
        starting_fname = sanitize_fname(record.company_name)
        fname = unused_fname(dir_, starting_fname).with_suffix('.txt')
        if director:
            print('{record.company_name} -> {fname.name}'.format(**locals()))
            with fname.open('w') as f:
              f.write(director)
        else:
            print('No departure of directors for {record.company_name}'.format(**locals()))


if __name__ == '__main__':
    year = 2008
    qtr = 1
    filterer = pick_n(n=100)
    dir_ = new_directory()
    print('results in', dir_)
    main(year, qtr, filterer, dir_)
