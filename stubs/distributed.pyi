from typing import TypeVar, Generic, List, Callable, Iterable


T = TypeVar('T')
U = TypeVar('U')


class Future(Generic[U]):
    ...


class Client:
    def __init__(self, **kwargs):
        ...

    def gather(self, futures: List[Future[U]]) -> List[U]:
        ...

    def map(self, func: Callable[[T], U], lst: Iterable[T]) -> List[Future[U]]:
        ...
