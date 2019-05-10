from typing import Iterable, Tuple, TypeVar


T = TypeVar('T')


def ngrams(num: int, seq: Iterable[T]) -> Iterable[Tuple[T]]:
    ...
