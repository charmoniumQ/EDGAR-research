from typing import Iterable
from dask.bag import Bag
from edgar_code.storage import PathLike


def bag2csv(path: PathLike, bag: Bag[Iterable[Iterable[str]]]) -> None:
    pass
