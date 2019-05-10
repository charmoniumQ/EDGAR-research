from typing import Iterable
from dask.bag import Bag
from edgar_code.gs_path import PathLike


def bag2csv(path: PathLike, bag: Bag[Iterable[Iterable[str]]]) -> None:
    pass
