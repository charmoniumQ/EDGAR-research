from typing import (
    TypeVar, Generic, List, Callable, Iterable, Any
)
from dask.bag import Bag


T = TypeVar('T')
U = TypeVar('U')


class Future(Generic[U]):
    def result(self) -> U:
        ...

class Client:
    def __init__(self, **kwargs: Any) -> None:
        ...

    def gather(self, futures: List[Future[U]]) -> List[U]:
        ...

    def map(self, func: Callable[[T], U], lst: Iterable[T]) -> List[Future[U]]:
        ...

    def compute(self, obj: Bag[T], sync: bool = ...) -> Future[T]:
        ...
