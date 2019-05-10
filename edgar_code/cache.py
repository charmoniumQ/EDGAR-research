from __future__ import annotations
import pickle
import shutil
import functools
import threading
import collections
from pathlib import Path
import urllib.parse
from typing import Callable, Any, TypeVar, cast, Tuple, Dict, Optional, Union, IO
import logging
from typing_extensions import Protocol


logging.basicConfig(level=logging.INFO)


class Serializer(Protocol):
    # pylint: disable=unused-argument,no-self-use
    def load(self, fil: IO[bytes]) -> Any:
        ...
    def dump(self, obj: Any, fil: IO[bytes]) -> None:
        ...


F2 = TypeVar('F2', bound=Callable[..., Any])
class Cache:
    @classmethod
    def decor(
            cls, obj_store: Callable[[str], ObjectStore],
            hit_msg: bool = False, miss_msg: bool = False, suffix: str = '',
    ) -> Callable[[F2], F2]:
        '''Decorator that creates a cached function

            >>> @Cache.decor(ObjectStore())
            >>> def foo():
            ...     pass

        '''
        def decor_(function: F2) -> F2:
            return cast(
                F2,
                functools.wraps(function)(
                    cls(obj_store, function, hit_msg, miss_msg, suffix)
                )
            )
        return decor_

    disabled: bool

    #pylint: disable=too-many-arguments
    def __init__(
            self, obj_store: Callable[[str], ObjectStore], function: F2,
            hit_msg: bool = False, miss_msg: bool = False, suffix: str = ''
    ) -> None:
        '''Cache a function.

        Note this uses `function.__qualname__` to determine the file
        name. If this is not unique within your program, define
        suffix.

        Note this uses `function.version` when defined, so objects of
        the same functions of different versions will not collide.

        '''

        self.function = function
        self.name = '-'.join(filter(bool, [
            self.function.__qualname__,
            suffix,
            getattr(self.function, 'version', ''),
        ]))
        self.obj_store = obj_store(self.name)
        self.hit_msg = hit_msg
        self.miss_msg = miss_msg
        self.sem = threading.RLock()
        self.__qualname__ = f'Cache({self.name})'
        self.disabled = False

    def __call__(self, *pos_args: Any, **kwargs: Any) -> Any:
        if self.disabled:
            return self.function(*pos_args, **kwargs)
        else:
            with self.sem:
                args_key = self.obj_store.to_key(pos_args, kwargs)
                if args_key in self.obj_store:
                    if self.hit_msg:
                        logging.info('hit %s with %s, %s',
                                     self.name, pos_args, kwargs)
                    res = self.obj_store[args_key]
                else:
                    if self.miss_msg:
                        logging.info('miss %s with %s, %s',
                                     self.name, pos_args, kwargs)
                    res = self.function(*pos_args, **kwargs)
                    self.obj_store[args_key] = res
                return res

    def clear(self) -> None:
        '''Removes all cached items'''
        self.obj_store.clear()

    def __str__(self) -> str:
        store_type = type(self.obj_store).__name__
        return f'Cache of {self.name} with {store_type}'


class ObjectStore(collections.UserDict): # type: ignore
    @classmethod
    def create(cls, *args: Any, **kwargs: Any) -> Callable[[str], ObjectStore]:
        '''Curried init. Name will be applied later.'''
        @functools.wraps(cls)
        def create_(name: str) -> ObjectStore:
            return cls(*args, name=name, **kwargs) # type: ignore
        return create_

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__()

    def to_key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        # pylint: disable=no-self-use
        # return hashable((args, kwargs))
        if kwargs:
            args = args + (kwargs,)
        return safe_name(args)


class FileStore(ObjectStore):
    '''An obj_store that persists at ./${CACHE_PATH}/${FUNCTION_NAME}_cache.pickle'''

    def __init__(
            self, cache_path: Path, name: str, serializer: Optional[Serializer] = None,
    ):
        super().__init__(name)
        self.serializer: Serializer = cast(
            Serializer,
            serializer if serializer is not None else pickle
        )
        self.cache_path = pathify(cache_path) / (self.name + '_cache.pickle')
        self.loaded = False
        self.data = {}

    def load_if_not_loaded(self) -> None:
        if not self.loaded:
            self.loaded = True
            if self.cache_path.exists():
                with self.cache_path.open('rb') as fil:
                    self.data = self.serializer.load(fil)
            else:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                self.data = {}

    def commit(self) -> None:
        self.load_if_not_loaded()
        if self.data:
            with self.cache_path.open('wb') as fil:
                self.serializer.dump(self.data, fil)
        else:
            if self.cache_path.exists():
                self.cache_path.unlink()

    def __setitem__(self, key: Any, obj: Any) -> None:
        self.load_if_not_loaded()
        super().__setitem__(key, obj)
        self.commit()

    def __delitem__(self, key: Any) -> None:
        self.load_if_not_loaded()
        super().__delitem__(key)
        self.commit()

    def clear(self) -> None:
        self.load_if_not_loaded()
        super().clear()
        self.commit()


# TODO: make this Path
Key = Any
class DirectoryStore(ObjectStore):
    '''Stores objects at ./${OBJ_PATH}/${FUNCTION_NAME}/${urlencode(args)}.pickle'''

    def __init__(
            self, object_path: Path, name: str,
            serializer: Optional[Serializer] = None
    ) -> None:
        super().__init__(name)
        self.serializer: Serializer = cast(
            Serializer,
            serializer if serializer is not None else pickle
        )
        self.obj_path = pathify(object_path) / self.name

    def to_key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Key:
        if kwargs:
            args = args + (kwargs,)
        fname = urllib.parse.quote(f'{safe_name(args)}.pickle', safe='')
        return self.obj_path / fname

    def __setitem__(self, path: Key, obj: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as fil:
            self.serializer.dump(obj, fil)

    def __delitem__(self, path: Key) -> None:
        path.unlink()

    def __getitem__(self, path: Key) -> Any:
        with path.open('rb') as fil:
            return self.serializer.load(fil)

    def __contains__(self, path: Key) -> bool:
        return bool(path.exists())

    def clear(self) -> None:
        if hasattr(self.obj_path, 'rmtree'):
            cast(Any, self.obj_path).rmtree()
        else:
            shutil.rmtree(self.obj_path)


def hashable(obj: Any) -> Any:
    '''Converts args and kwargs into a hashable type (overridable)'''
    try:
        hash(obj)
    except TypeError:
        if hasattr(obj, 'items'):
            # turn dictionaries into frozenset((key, val))
            return tuple(sorted((key, hashable(val))) for key, val in obj.items())
        elif hasattr(obj, '__iter__'):
            # turn iterables into tuples
            return tuple(hashable(val) for val in obj)
        else:
            raise TypeError(f"I don't know how to hash {obj} ({type(obj)})")
    else:
        return obj


def safe_name(obj: Any) -> str:
    '''
Safe names are compact, unique, urlsafe, and equal when the objects are equal

str does not work because x == y does not imply str(x) == str(y).

    >>> a = dict(d=1, e=1)
    >>> b = dict(e=1, d=1)
    >>> a == b
    True
    >>> str(a) == str(b)
    False
    >>> safe_str(a) == safe_str(b)
    True
'''
    if isinstance(obj, int):
        return str(obj)
    elif isinstance(obj, float):
        return str(round(obj, 3))
    elif isinstance(obj, str):
        return urllib.parse.quote(repr(obj))
    elif isinstance(obj, tuple):
        return '%2C'.join(map(safe_name, obj))
    elif isinstance(obj, dict):
        contents = '%2C'.join(
            safe_name(key) + '%3A' + safe_name(val)
            for key, val in sorted(obj.items())
        )
        return '%7B' + contents + '%7D'
    else:
        raise TypeError()


def pathify(obj: Union[str, Path]) -> Path:
    if isinstance(obj, str):
        return Path(obj)
    else:
        return obj
