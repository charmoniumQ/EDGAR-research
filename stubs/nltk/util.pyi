from typing import Iterable, Tuple, TypeVar, List


T = TypeVar('T')


def ngrams(seq: List[T], num: int) -> List[Tuple[T, ...]]:
    ...
