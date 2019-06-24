from typing import (
    TypeVar, Generic, List, Callable, Iterable, Any, Union, ContextManager
)
from dask.bag import Bag


FutureT = TypeVar('FutureT')
class Future(Generic[FutureT]):
    def result(self) -> FutureT:
        ...

class LocalCluster:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

T = TypeVar('T')
U = TypeVar('U')
class Client(ContextManager[None]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def wait_for_workers(self, n_workers: int) -> None:
        ...

    def gather(self, futures: List[Future[U]]) -> List[U]:
        ...

    def map(self, func: Callable[[T], U], lst: Iterable[T]) -> List[Future[U]]:
        ...

    def compute(self, obj: Any, sync: bool = ...) -> Any:
        ...

    def upload_file(self, path: str) -> None:
        ...
