import threading
import collections
from pathlib import Path
import urllib
import logging
logging.basicConfig(level=logging.INFO)
import shutil
import random
import string
import abc


sem = threading.BoundedSemaphore(value=1)


# TODO: allow caching 'named objects'. Cache the name instead of the object.
# TODO: allow caching objects by provenance. Cache the thing you did to make the object instead of the object.
# https://github.com/bmabey/provenance
# TODO: functools.wraps
# TODO: make the obj_stores (list of obj_store factories) more intuitive, easier for developers to write


class Cache(object):
    @classmethod
    def decor(Class, obj_store, hit_msg=False, miss_msg=False, suffix=''):
        '''Decorator that creates a cached function

            @Cache.decor(obj_store)
            def foo():
                pass

        '''
        def decor_(function):
            return Class(obj_store, function, hit_msg, miss_msg, suffix='')
        return decor_

    def __init__(self, obj_store, function, hit_msg=False, miss_msg=False, suffix=''):
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

    def __call__(self, *pos_args, **kwargs):
        with sem:
            args_key = self.obj_store.to_key(pos_args, kwargs)
            if args_key in self.obj_store:
                if self.hit_msg:
                    logging.info(f'hit {self.name} with {pos_args}, {kwargs}')
                res = self.obj_store[args_key]
            else:
                if self.miss_msg:
                    logging.info(f'miss {self.name} with {pos_args}, {kwargs}')
                res = self.function(*pos_args, **kwargs)
                self.obj_store[args_key] = res
            return res

    def clear(self):
        '''Removes all cached items'''
        self.obj_store.clear()

    def __str__(self):
        store_type = type(self.obj_store).__name__
        return f'Cache of {self.name} with x{store_type}'


class ObjectStore(collections.UserDict):
    @classmethod
    def create(Class, *args, **kwargs):
        '''Curried init. Name will be applied later.'''
        def create_(name):
            return Class(*args, name=name, **kwargs)
        return create_

    def __init__(self, name):
        self.name = name
        super().__init__()

    @classmethod
    def to_key(Class, args, kwargs):
        # return hashable((args, kwargs))
        if kwargs:
            args = args + (kwargs,)
        return safe_name(args)


class FileStore(ObjectStore):
    '''An obj_store that persists at ./${CACHE_PATH}/${FUNCTION_NAME}_cache.pickle'''

    def __init__(self, cache_path, name, serializer=None, load_msg=False):
        super().__init__(name)
        if serializer is None:
            import pickle
            serializer = pickle
        self.serializer = serializer
        self.cache_path = pathify(cache_path) / (self.name + '_cache.pickle')

        if self.cache_path.exists():
            with self.cache_path.open('rb') as f:
                self.data = self.serializer.load(f)
        else:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.data = {}

    def commit(self):
        if self.data:
            with self.cache_path.open('wb') as f:
                self.serializer.dump(self.data, f)
        else:
            if self.cache_path.exists():
                self.cache_path.unlink()

    def __setitem__(self, key, obj):
        super().__setitem__(key, obj)
        self.commit()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.commit()

    def clear(self):
        super().clear()
        self.commit()


class DirectoryStore(ObjectStore):
    '''Stores objects at ./${OBJ_PATH}/${FUNCTION_NAME}/${urlencode(args)}.pickle'''

    def __init__(self, object_path, name, serializer=None):
        super().__init__(name)
        if serializer is None:
            import pickle
            serializer = pickle
        self.serializer = serializer
        self.obj_path = pathify(object_path) / self.name

    def to_key(self, args, kwargs):
        if kwargs:
            args = args + (kwargs,)
        fname = urllib.parse.quote(f'{safe_name(args)}.pickle', safe='')
        return self.obj_path / fname

    def __setitem__(self, path, obj):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as f:
            self.serializer.dump(obj, f)

    def __delitem__(self, path):
        path.unlink()

    def __getitem__(self, path):
        with path.open('rb') as f:
            return self.serializer.load(f)

    def __contains__(self, path):
        return path.exists()

    def clear(self):
        if hasattr(self.obj_path, 'rmtree'):
            self.obj_path.rmtree
        else:
            shutil.rmtree(self.obj_path)


def hashable(obj):
    '''Converts args and kwargs into a hashable type (overridable)'''
    try:
        hash(obj)
    except:
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


def safe_name(obj):
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


def pathify(obj):
    if isinstance(obj, (str, bytes)):
        return Path(obj)
    else:
        return obj


if __name__ == '__main__':
    import shutil
    import tempfile

    with tempfile.TemporaryDirectory() as cache_dir:
        calls = []

        @Cache.decor(ObjectStore.create())
        def square1(x):
            calls.append(x)
            return x**2

        @Cache.decor(FileStore.create('cache/'))
        def square2(x):
            calls.append(x)
            return x**2

        @Cache.decor(DirectoryStore.create('cache/'), hit_msg=True, miss_msg=True)
        def square3(x):
            calls.append(x)
            return x**2

        for square in [square1, square2, square3]:
            calls.clear()
            square(7) # miss
            square(2) # miss
            square(7)
            square(2)
            square.clear()
            square(7) # miss

        assert calls == [7, 2, 7]
