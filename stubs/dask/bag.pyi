from __future__ import annotations
from typing import (
    TypeVar, Generic, Iterable, Callable, Type, Any, Tuple, Iterator, List
)
from dask.highlevelgraph import HighLevelGraph


T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class Bag(Generic[T]):
    dask: HighLevelGraph
    name: str
    npartitions: int
    def __init__(
            self, dask: HighLevelGraph, name: str, npartitions: int
    ) -> None:
        ...

    def map(self, f: Callable[[T], U]) -> Bag[U]:
        ...

    def compute(self) -> List[T]:
        ...

    def take(self, n: int, compute: bool = ..., npartitions: int = ...) -> List[T]:
        ...

    def map_partitions(self, func: Callable[[List[T]], List[U]]) -> Bag[U]:
        ...

    def filter(self, pred: Callable[[T], bool]) -> Bag[T]:
        ...

    def reduction(
            self,
            perpartition: Callable[[Iterable[T]], U],
            aggregate: Callable[[Iterable[U]], V],
    ) -> V:
        ...

def from_sequence(sequence: Iterable[T], npartitions: int = ...) -> Bag[T]:
    ...

def concat(bags: Iterable[Bag[T]]) -> Bag[T]:
    ...

# zip is variadic, but in order to type this properly, I will write
# the signature for zip2
def zip(bag1: Bag[T], bag2: Bag[U]) -> Bag[Tuple[T, U]]:
    ...

def map(func: Callable[[T, U], V], bag1: Bag[T], bag2: Bag[U]) -> Bag[V]:
    ...

def range(a: int, b: int = ..., step: int = ..., npartitions: int = ...) -> Bag[int]:
    ...
