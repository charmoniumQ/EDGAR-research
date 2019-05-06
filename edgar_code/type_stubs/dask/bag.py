from typing import TypeVar, Generic, Iterable

T = TypeVar('T')

class Bag(Generic[T]):
    ...

def from_sequence(sequence: Iterable[T]) -> Bag[T]:
    ...
