import pickle
import shutil
import threading
import collections
from pathlib import Path
import urllib.parse
from typing import Callable, Any, TypeVar, cast, Tuple, Dict, Optional, Union
import logging
from typing_extensions import Protocol


logging.basicConfig(level=logging.INFO)


FuncType = Callable[..., Any]
class Hashable(Protocol): # pylint: disable=too-few-public-methods
    ...
class BinaryFile(Protocol): # pylint: disable=too-few-public-methods
    ...
class Serializer(Protocol):
    # pylint: disable=unused-argument,no-self-use
    def load(self, fil: BinaryFile) -> Any:
        ...
    def dump(self, obj: Any, fil: BinaryFile) -> None:
        ...

# TODO: allow caching 'named objects'. Cache the name instead of the object.
# TODO: allow caching objects by provenance. Cache the thing you did to make the object instead of the object.
# https://github.com/bmabey/provenance
# TODO: functools.wraps
# TODO: make the obj_stores (list of obj_store factories) more intuitive, easier for developers to write


F1 = TypeVar('F1', bound=FuncType)
def make_lazy_callable(make_callable: F1) -> F1:
    callablef = None
    def lazy_f(*args, **kwargs):
        if callablef is None:
            callabelf = make_callable()
        return callabelf(*args, **kwargs)
    return cast(F1, lazy_f)


class ObjectStore(collections.UserDict): #pylint: disable=too-many-ancestors
    @classmethod
    def create(cls, *args, **kwargs):
        '''Curried init. Name will be applied later.'''
        def create_(name):
            return cls(*args, name=name, **kwargs)
        return create_

    def __init__(self, name: str):
        self.name = name
        super().__init__()

    def to_key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        # pylint: disable=no-self-use
        # return hashable((args, kwargs))
        if kwargs:
            args = args + (kwargs,)
        return safe_name(args)


F2 = TypeVar('F2', bound=FuncType)
class Cache:
    @classmethod
    def decor(cls, obj_store: Callable[[str], ObjectStore], hit_msg=False,
              miss_msg=False, suffix='') -> Callable[[F2], F2]:
        '''Decorator that creates a cached function

            @Cache.decor(obj_store)
            def foo():
                pass

        '''
        def decor_(function: F2) -> F2:
            return cast(F2, cls(obj_store, function, hit_msg, miss_msg, suffix))
        return decor_

    #pylint: disable=too-many-arguments
    def __init__(self, obj_store: Callable[[str], ObjectStore], function: F2,
                 hit_msg=False, miss_msg=False, suffix=''):
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

    def __call__(self, *pos_args, **kwargs) -> Any:
        with self.sem:
            args_key = self.obj_store.to_key(pos_args, kwargs)
            if args_key in self.obj_store:
                if self.hit_msg:
                    logging.info('hit %s with %s, %s', self.name, pos_args, kwargs)
                res = self.obj_store[args_key]
            else:
                if self.miss_msg:
                    logging.info('miss %s with %s, %s', self.name, pos_args, kwargs)
                res = self.function(*pos_args, **kwargs)
                self.obj_store[args_key] = res
            return res

    def clear(self) -> None:
        '''Removes all cached items'''
        self.obj_store.clear()

    def __str__(self) -> str:
        store_type = type(self.obj_store).__name__
        return f'Cache of {self.name} with {store_type}'


class FileStore(ObjectStore): # pylint: disable=too-many-ancestors
    '''An obj_store that persists at ./${CACHE_PATH}/${FUNCTION_NAME}_cache.pickle'''

    def __init__(self, cache_path: Path, name: str, serializer: Optional[Serializer] = None):
        super().__init__(name)
        self.serializer: Serializer = cast(
            Serializer,
            serializer if serializer is not None else pickle
        )
        self.cache_path = pathify(cache_path) / (self.name + '_cache.pickle')

        if self.cache_path.exists():
            with self.cache_path.open('rb') as fil:
                self.data = self.serializer.load(fil)
        else:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.data = {}

    def commit(self) -> None:
        if self.data:
            with self.cache_path.open('wb') as fil:
                self.serializer.dump(self.data, fil)
        else:
            if self.cache_path.exists():
                self.cache_path.unlink()

    def __setitem__(self, key: Hashable, obj: Any) -> None:
        super().__setitem__(key, obj)
        self.commit()

    def __delitem__(self, key: Hashable) -> None:
        super().__delitem__(key)
        self.commit()

    def clear(self) -> None:
        super().clear()
        self.commit()


# TODO: make this Path
Key = Any
class DirectoryStore(ObjectStore): # pylint: disable=too-many-ancestors
    '''Stores objects at ./${OBJ_PATH}/${FUNCTION_NAME}/${urlencode(args)}.pickle'''

    def __init__(self, object_path: Path, name: str,
                 serializer: Optional[Serializer] = None):
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
        return path.exists()

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
