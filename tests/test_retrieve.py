from typing import cast
import distributed
import dask.bag
import pytest
from edgar_code.cache import Cache
import edgar_code.retrieve as retrieve


@pytest.mark.slow
def test_retrieve() -> None:
    cluster = distributed.LocalCluster(
        ip='localhost:8786',
        # I want a bokeh interface to check progress
        dashboard_address='localhost:8787',
        # single process, single thread allows ctrl+C backtrace to
        # show where the code is getting stuck. Otherwise, it will say,
        # "I'm stuck waiting for other processes." It also makes
        # time_code more meaningful
        processes=False,
        threads_per_worker=1,
    )
    # TODO: put this in a reusable module

    with distributed.Client(cluster):

        # disable the cache, because I don't want to persist these results
        # in the cloud
        for cached_func in [retrieve.get_rfs, retrieve.get_paragraphs,
                            retrieve.get_raw_forms, retrieve.get_indexes]:
            assert isinstance(cached_func, Cache)
            cast(Cache, cached_func).disabled = True

        rfs = dask.bag.zip( # pylint: disable=unused-variable
            retrieve.get_indexes('10-K', 1995, 1),
            retrieve.get_rfs(1995, 1)
        ).take(10, npartitions=1)

        # for index, rf in rfs:
        #     if isinstance(rf, Exception):
        #         print(index.url)
        #         print(str(rf))
        #         print()
        #     else:
        #         print(index.url)
        #         print(sum(map(len, rf)))
        #         print()


if __name__ == '__main__':
    from edgar_code.util import time_code
    test_retrieve()
    time_code.print_stats()
