from typing import Union, List, Tuple, Callable, Generator, TypeVar, cast
import tempfile
import contextlib
import csv
from pathlib import Path
import logging
import matplotlib.pyplot as plt
import numpy as np
import dask.bag
import edgar_code.cli.config as config
from edgar_code.gs_path import copy
from edgar_code.util import time_code
from edgar_code.retrieve import get_rfs, get_indexes
from edgar_code.types import PathLike, Bag


# TODO: combine with retrieve.py


logger = logging.getLogger(__name__)


def rf_mapper(rf: Union[List[str], Exception]) -> Tuple[int, int, str]:
    if isinstance(rf, list):
        return (
            sum(map(len, rf)),
            len(rf),
            rf[0][:50] + ' ... ' + rf[1][-50:],
        )
    else:
        return (-1, -1, repr(rf))

T = TypeVar('T')
def time_code_getter(val: T) -> T:
    print(time_code.format_stats())
    return val

@time_code.decor(print_start=True, print_time=True, print_args=True)
def get_bag(year: int, qtr: int) -> Bag[Tuple[str, int, int, str]]:
    return dask.bag.map(
        lambda index, rf_count: (index.url, rf_count[0], rf_count[1], rf_count[2]),
        get_indexes('10-K', year, qtr),
        get_rfs(year, qtr).map(rf_mapper),
    )


@contextlib.contextmanager
def get_styled_ax(path: PathLike) -> Generator[plt.Axes, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir_:
        temp_dir = Path(temp_dir_)
        chars_file = temp_dir / 'plot.png'
        fig = plt.figure()
        ax = fig.gca()
        yield ax
        fig.savefig(chars_file)
        plt.close(fig)
        copy(chars_file, path)


def main() -> None:
    client = config.get_client()
    logger.debug('using client: %r', client)

    # This comment is commented out
    # # submiting all bags for computation at once loads the cluster
    # # more efficiently. Workers don't have to idle in between bags
    # # being submitted; there is always more work available.
    # before uncommenting generator->list

    get_bag(1995, 1).compute()
    logger.debug('here')
    get_bag(1995, 2).compute()

    future_bags = [
        ((year, qtr), get_bag(year, qtr).compute())
        for year in range(1993, 2019)
        for qtr in range(1, 5)
    ]

    all_chars: List[int] = []
    all_paragraphs: List[int] = []
    alltime_path = config.results_path / 'get_all_rfs/good_stats.txt'
    with alltime_path.open('w') as alltime_file:
        for (year, qtr), future_bag in future_bags:
            quarter_dir = config.results_path / 'get_all_rfs/{year}_{qtr}/'

            # This is also commented out
            # # I am calling .result instead of gather because the whole
            # # thing might not fit in RAM on the workers.  When I consume
            # # one result, it frees up that memory, so they can compute the
            # # next one. It also is less memory intensive on this pod.
            with time_code.ctx(
                    f'fetching get_rfs({year}, {qtr})', print_start=True, print_time=True
            ):
                # bag = future_bag.result(timeout=None)
                bag = future_bag

            with time_code.ctx(f'write for {year} {qtr}', print_start=True, print_time=True):
                urls, chars, paragraphs, texts = (
                    [''] * len(bag),
                    np.zeros(len(bag), dtype=int),
                    np.zeros(len(bag), dtype=int),
                    [''] * len(bag),
                )
                for i, row in enumerate(bag):
                    urls[i] = row[0]
                    chars[i] = row[1]
                    paragraphs[i] = row[2]
                    texts[i] = row[3]

                all_chars.extend(chars)
                all_paragraphs.extend(paragraphs)

                with (quarter_dir / 'results.txt').open('w') as results_file:
                    csvw = csv.writer(results_file)
                    csvw.writerow(zip(urls, texts))

                total = len(bag)
                good = sum(map(
                    # cannot type lambdas; must use cast :(
                    cast(Callable[[int], int], lambda x: x != -1),
                    paragraphs
                ))
                print(f'For: {year} {qtr}', file=alltime_file)
                print(f'Good: {good / total * 100:.0f}%', file=alltime_file)
                print('', file=alltime_file)

    with get_styled_ax(config.results_path / 'get_all_rfs/chars.png') as ax:
        ax.plot(range(len(chars)), sorted(chars))
        ax.set_xlabel('# of chars')
        ax.set_ylabel('# of docs')

    with get_styled_ax(config.results_path / 'get_all_rfs/paragraphs.png') as ax:
        ax.plot(range(len(paragraphs)), sorted(paragraphs))
        ax.set_xlabel('# of chars')
        ax.set_ylabel('# of docs')

if __name__ == '__main__':
    main()
