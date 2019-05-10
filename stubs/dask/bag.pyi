from __future__ import annotations
from typing import (
    TypeVar, Generic, Iterable, Callable, Type, Any, Tuple, Iterator, List
)


T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class Bag(Generic[T]):
    def map(self, f: Callable[[T], U]) -> Bag[U]:
        ...

    def compute(self) -> Iterable[T]:
        ...
    def take(self, n: int) -> List[T]:
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
