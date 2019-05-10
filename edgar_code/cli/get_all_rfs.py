from typing import Union, List
import csv
import dask.bag
import edgar_code.config as config
from edgar_code.util import time_code
from edgar_code.retrieve import get_rfs, get_indexes
from edgar_code.cli.get_client import get_client

def main() -> None:
    client = get_client()

    def rf_mapper(rf: Union[List[str], Exception]) -> List[str]:
        if isinstance(rf, list):
            return [str(sum(map(len, rf))), rf[0][:50] + ' ... ' + rf[1][-50:]]
        else:
            return ['', repr(rf)]

    # submiting all bags for computation at once loads the cluster
    # more efficiently. Workers don't have to idle in between bags
    # being submitted; there is always more work available.
    future_bags = [
        ((year, qtr), client.compute(
            dask.bag.map(
                lambda a, b: a + b,
                get_indexes('10-K', year, qtr).map(lambda index: [index.url]),
                get_rfs(year, qtr).map(rf_mapper),
            ),
            sync=False))
        for year in range(1993, 2019)
        for qtr in range(1, 5)
    ]

    for (year, qtr), future_bag in future_bags:
        # I am calling .result instead of gather because the whole
        # thing might not fit in RAM on the workers.  When I consume
        # one result, it frees up that memory, so they can compute the
        # next one. It also is less memory intensive on this pod.
        with time_code.ctx(f'get and write rfs for {year} {qtr}'):
            bag = future_bag.result()
            with config.results_path.open('w') as fil:
                csvw = csv.writer(fil)
                csvw.writerows(bag)


if __name__ == '__main__':
    main()
