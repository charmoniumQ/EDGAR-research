from typing import (
    List, Iterable, TypeVar, Callable, Optional, Iterator, Dict, Any

)
import functools


T = TypeVar('T')
U = TypeVar('U')


def generator2fn_list(generator: Callable[..., Iterable[T]]) \
    -> Callable[..., List[T]]:
    ''''Use as an annotation


    >>> @generator2fn_list
    ... def gen():
    ...     yield 2
    ...     yield 3
    >>> gen()
    ... [2, 3]

     '''

    @functools.wraps(generator)
    def func(*args: Any, **kwargs: Any) -> List[T]:
        return list(generator(*args, **kwargs))
    return func


def invert(dct: Dict[T, U]) -> Dict[U, T]:
    return {value: key for key, value in dct.items()}


def generator2iterator(generator: Callable[[], Iterable[T]],
                       length: Optional[int] = None) -> Iterable[T]:

    '''Generator -> restartable iterator that knows its len after the first iteration'''
    class MyIterator:
        def __init__(self) -> None:
            self.length = length
        def __iter__(self) -> Iterator[T]:
            if self.length is not None:
                yield from iter(generator())
            else:
                length = 0
                for elem in iter(generator()):
                    length += 1
                    yield elem
                self.length = length
        def __len__(self) -> int:
            if self.length is not None:
                return self.length
            else:
                raise RuntimeError('len not known yet')
    return MyIterator()

def concat_lists(lists: Iterable[List[T]]) -> List[T]:
    ret = []
    for list_ in lists:
        ret.extend(list_)
    return ret

def merge_dicts(dicts: Iterable[Dict[T, U]]) -> Dict[T, U]:
    ret = {}
    for dict_ in dicts:
        ret.update(dict_)
    return ret


__all__ = [
    'merge_dicts', 'concat_lists', 'generator2iterator',
    'invert', 'generator2fn_list',
]
