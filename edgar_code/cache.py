from __future__ import annotations
import abc
import shutil
import functools
from pathlib import Path
import urllib.parse
from typing import (
    Callable, Any, TypeVar, cast, Tuple, Dict, Optional,
    Union, Hashable,
)
import logging
from edgar_code.types import PathLike, Serializer, UserDict
from edgar_code.util.picklable_threading import RLock


logger = logging.getLogger(__name__)


CacheKey = TypeVar('CacheKey')
CacheReturn = TypeVar('CacheReturn')
CacheFunc = TypeVar('CacheFunc', bound=Callable[..., Any])
class Cache:
    @classmethod
    def decor(
            cls,
            obj_store: Callable[[str], ObjectStore[CacheKey, CacheReturn]],
            hit_msg: bool = False, miss_msg: bool = False, suffix: str = '',
    ) -> Callable[[CacheFunc], CacheFunc]:
        '''Decorator that creates a cached function

            >>> @Cache.decor(ObjectStore())
            >>> def foo():
            ...     pass

        '''
        def decor_(function: CacheFunc) -> CacheFunc:
            return cast(
                CacheFunc,
                functools.wraps(function)(
                    cls(obj_store, function, hit_msg, miss_msg, suffix)
                )
            )
        return decor_

    disabled: bool

    #pylint: disable=too-many-arguments
    def __init__(
            self,
            obj_store: Callable[[str], ObjectStore[CacheKey, CacheReturn]],
            function: CacheFunc,
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
        self.sem = RLock()
        self.__qualname__ = f'Cache({self.name})'
        self.disabled = False

    def __call__(self, *pos_args: Any, **kwargs: Any) -> Any:
        if self.disabled:
            return self.function(*pos_args, **kwargs)
        else:
            with self.sem:
                args_key = self.obj_store.args2key(pos_args, kwargs)
                if args_key in self.obj_store:
                    if self.hit_msg:
                        logger.info('hit %s with %s, %s',
                                    self.name, pos_args, kwargs)
                    res = self.obj_store[args_key]
                else:
                    if self.miss_msg:
                        logger.info('miss %s with %s, %s',
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


ObjectStoreKey = TypeVar('ObjectStoreKey')
ObjectStoreValue = TypeVar('ObjectStoreValue')
class ObjectStore(UserDict[ObjectStoreKey, ObjectStoreValue], abc.ABC):
    @classmethod
    def create(
            cls, *args: Any, **kwargs: Any
    ) -> Callable[[str], ObjectStore[ObjectStoreKey, ObjectStoreValue]]:
        '''Curried init. Name will be applied later.'''
        @functools.wraps(cls)
        def create_(name: str) -> ObjectStore[ObjectStoreKey, ObjectStoreValue]:
            return cls(*args, name=name, **kwargs) # type: ignore
        return create_

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    @abc.abstractmethod
    def args2key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> ObjectStoreKey:
        # pylint: disable=unused-argument,no-self-use
        ...


class MemoryStore(ObjectStore[Hashable, Any]):
    def __init__(self, name: str):
        # pylint: disable=non-parent-init-called
        ObjectStore.__init__(self, name)

    def args2key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Hashable:
        # pylint: disable=no-self-use
        return to_hashable((args, kwargs))


class FileStore(MemoryStore):
    '''An obj_store that persists at ./${CACHE_PATH}/${FUNCTION_NAME}_cache.pickle'''

    def __init__(
            self, cache_path: PathLike, name: str, serializer: Optional[Serializer] = None,
    ):
        # pylint: disable=non-parent-init-called,super-init-not-called
        ObjectStore.__init__(self, name)
        if serializer is None:
            import pickle
            self.serializer = cast(Serializer, pickle)
        else:
            self.serializer = serializer
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

    def args2key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Hashable:
        # pylint: disable=no-self-use
        return to_hashable((args, kwargs))

    def commit(self) -> None:
        self.load_if_not_loaded()
        if self.data:
            with self.cache_path.open('wb') as fil:
                self.serializer.dump(self.data, fil)
        else:
            if self.cache_path.exists():
                print('deleting ', self.cache_path)
                self.cache_path.unlink()

    def __setitem__(self, key: Hashable, obj: Any) -> None:
        self.load_if_not_loaded()
        super().__setitem__(key, obj)
        self.commit()

    def __delitem__(self, key: Hashable) -> None:
        self.load_if_not_loaded()
        super().__delitem__(key)
        self.commit()

    def clear(self) -> None:
        self.load_if_not_loaded()
        super().clear()
        self.commit()


class DirectoryStore(ObjectStore[PathLike, Any]):
    '''Stores objects at ./${CACHE_PATH}/${FUNCTION_NAME}/${urlencode(args)}.pickle'''

    def __init__(
            self, object_path: PathLike, name: str,
            serializer: Optional[Serializer] = None
    ) -> None:
        # pylint: disable=non-parent-init-called
        ObjectStore.__init__(self, name)
        if serializer is None:
            import pickle
            self.serializer = cast(Serializer, pickle)
        else:
            self.serializer = serializer
        self.cache_path = pathify(object_path) / self.name

    def args2key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> PathLike:
        if kwargs:
            args = args + (kwargs,)
        fname = urllib.parse.quote(f'{safe_str(args)}.pickle', safe='')
        return self.cache_path / fname

    def __setitem__(self, path: PathLike, obj: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as fil:
            self.serializer.dump(obj, fil)

    def __delitem__(self, path: PathLike) -> None:
        path.unlink()

    def __getitem__(self, path: PathLike) -> Any:
        with path.open('rb') as fil:
            return self.serializer.load(fil)

    def __contains__(self, path: Any) -> bool:
        if hasattr(path, 'exists'):
            return bool(path.exists())
        else:
            return False

    def clear(self) -> None:
        print('deleting')
        if hasattr(self.cache_path, 'rmtree'):
            cast(Any, self.cache_path).rmtree()
        else:
            shutil.rmtree(str(self.cache_path))


def to_hashable(obj: Any) -> Hashable:
    '''Converts args and kwargs into a hashable type (overridable)'''
    try:
        hash(obj)
    except TypeError:
        if hasattr(obj, 'items'):
            # turn dictionaries into frozenset((key, val))
            # sorting is necessary to make equal dictionaries map to equal things
            # sorted(..., key=hash)
            return tuple(sorted(
                [(keyf, to_hashable(val)) for keyf, val in obj.items()],
                key=hash
            ))
        elif hasattr(obj, '__iter__'):
            # turn iterables into tuples
            return tuple(to_hashable(val) for val in obj)
        else:
            raise TypeError(f"I don't know how to hash {obj} ({type(obj)})")
    else:
        return cast(Hashable, obj)


def safe_str(obj: Any) -> str:
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
        ret = str(obj)
    elif isinstance(obj, float):
        ret = str(round(obj, 3))
    elif isinstance(obj, str):
        ret = repr(obj)
    elif isinstance(obj, list):
        ret = '[' + ','.join(map(safe_str, obj)) + ']'
    elif isinstance(obj, tuple):
        ret = '(' + ','.join(map(safe_str, obj)) + ')'
    elif isinstance(obj, dict):
        ret = '{' + ','.join(sorted(
            safe_str(key) + ':' + safe_str(val)
            for key, val in obj.items()
        )) + '}'
    else:
        raise TypeError()
    return urllib.parse.quote(ret, safe='')


def pathify(obj: Union[str, PathLike]) -> PathLike:
    if isinstance(obj, str):
        return Path(obj)
    else:
        return obj
