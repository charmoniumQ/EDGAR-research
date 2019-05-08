from __future__ import annotations
from typing import TypeVar, Generic, Iterable, Callable, Type


T = TypeVar('T')
U = TypeVar('U')
class Bag(Generic[T]):
    def map(self, f: Callable[[T], U]) -> Bag[U]:
        ...


def from_sequence(sequence: Iterable[T], npartitions: int = 100) -> Bag[T]:
    ...
