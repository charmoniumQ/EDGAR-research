import collections
from pathlib import Path
import logging
import shutil
import random
import string
import abc


# TODO: allow caching 'named objects'. Cache the name instead of the object.
# TODO: allow caching objects by provenance. Cache the thing you did to make the object instead of the object.
# https://github.com/bmabey/provenance
# TODO: functools.wraps
# TODO: make the obj_stores (list of obj_store factories) more intuitive, easier for developers to write


class Cache(object):
    @classmethod
    def decor(Class, index, obj_stores, hit_msg=None, miss_msg=None, suffix=''):
        '''Decorator that creates a cached function

            @Cache.decor(index, store)
            def foo():
                pass

        '''
        def decor_(function):
            return Class(index, obj_stores, function, hit_msg=None, miss_msg=None, suffix='')
        return decor_

    def __init__(self, index, obj_stores, function, hit_msg=None, miss_msg=None, suffix=''):
        '''Cache a function.

        The store is a map from key -> function_returned_object where
        the key is determined entirely by the store, like a bag-check
        system.

        The index is a map from hashable function_arguments -> key (as
        determined by store).

        This lets us have indirection: the index to the cache is stored
        separately from the (potentially large) objects in the cache.

        If you do not need this indirection, just use NoStore() as the
        store.

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
        self.index = index(self.name)
        self.obj_stores = [
            obj_store(self.name) for obj_store in obj_stores
        ]
        self.hit_msg = hit_msg
        self.miss_msg = miss_msg

    def __call__(self, *args, **kwargs):
        key = self.hashable_args(*args, **kwargs)
        if key in self.index:
            if self.hit_msg:
                logging.info(f'hit {name} with {key}')
            res = self.store_get(self.index[key])
        else:
            if self.miss_msg:
                logging.info(f'miss {name} with {key}')
            res = self.function(*args, **kwargs)
            self.index[key] = self.store_put(res)
        return res

    def store_get(self, i_obj_store_key):
        i, obj_store_key = i_obj_store_key
        return self.obj_stores[i][obj_store_key]

    def store_put(self, obj):
        for i, obj_store in enumerate(self.obj_stores):
            if obj_store.can_store(obj):
                obj_store_key = obj_store.put(obj)
                return (i, obj_store_key)
        else:
            raise TypeError(f'no obj_store for {obj!s}')

    @staticmethod
    def hashable_args(*args, **kwargs):
        return hashable((args, kwargs))

    def clear(self):
        '''Removes all cached items'''
        for i, obj_store_key in self.index.values():
            del self.obj_stores[i][obj_store_key]

        if hasattr(self.index, 'clear'):
            self.index.clear()
        else:
            for key in self.index:
                del self.index[key]

    def __str__(self):
        index_type = type(self.index).__name__
        store_type = type(self.obj_store).__name__
        return f'Cache of {self.name} with {index_type} and {store_type}'


class Store(abc.ABC):
    Class = None
    Key = None

    @classmethod
    def create(Class, *args, **kwargs) -> Class:
        '''Curried init. Name will be applied later.'''
        def create_(name):
            return Class(*args, name=name, **kwargs)
        return create_

    def __init__(self, name) -> Key:
        self.name = name

    @abc.abstractmethod
    def can_store(self, obj):
        return False

    @abc.abstractmethod
    def put(self, obj):
        '''stores the obj AND returns its key for later'''
        pass

    @abc.abstractmethod
    def __getitem__(self, key: Key):
        pass

    @abc.abstractmethod
    def __delitem__(self, key: Key):
        '''clears one items from this store'''
        pass

    # def clear(self):
    #     '''clears all items from this store'''
    #     pass


class NoStore(Store):
    '''Uses the objects as keys.

    This has the effect that the index will end up storing the
    objects.

    '''

    def __init__(self, name, can_store=None):
        super().__init__(name)
        self.can_store_ = can_store if can_store is not None else lambda obj: True

    def can_store(self, obj):
        return self.can_store_(obj)

    def put(self, obj):
        return obj

    def __getitem__(self, key):
        return key

    def __delitem__(self, key):
        pass

    def clear(self):
        pass


class FileStore(Store):
    '''Store serialized objects as their own file.

    Good for large objects.

    Stores at ./${PARENT_DIR}/${FUNCTION_NAME}/${RAND_STRING}

    '''

    def __init__(self, parent_dir, name, serializer=None):
        if serializer is None:
            import pickle
            serializer = pickle
        super().__init__(name)
        self.path = pathify(parent_dir) / self.name
        self.path.mkdir(exist_ok=True, parents=True)
        self.serializer = serializer

    def can_store(self, obj):
        return True

    def gen_key(self):
        return rand_name()

    def put(self, obj):
        while True:
            key = self.gen_key()
            fname = self.path / key
            if not fname.exists():
                break

        with fname.open('wb') as f:
            self.serializer.dump(obj, f)
        return fname

    def __getitem__(self, fname):
        with fname.open('rb') as f:
            return self.serializer.load(f)

    def __delitem__(self, fname):
        if fname.exists():
            fname.unlink()

    def clear(self):
        if self.path.exists():
            if hasattr(self.path, 'rmtree'):
                self.path.rmtree()
            else:
                shutil.rmtree(self.path)
        self.path.mkdir(parents=True)


class Index(abc.ABC):
    @classmethod
    def create(Class, *args, **kwargs):
        def create_(name):
            return Class(*args, name=name, **kwargs)
        return create_

    def __init__(self, name):
        self.name = name

    @abc.abstractmethod
    def __setitem__(self, key, val):
        pass

    @abc.abstractmethod
    def __getitem__(self, key):
        pass

    @abc.abstractmethod
    def __delitem__(self, key):
        pass

    @abc.abstractmethod
    def clear(self):
        pass


class IndexInRam(collections.UserDict, Index):
    '''An index with no persistence'''
    def __init__(self, name):
        Index.__init__(self, name)
        collections.UserDict.__init__(self)


class IndexInFile(collections.UserDict, Index):
    '''An index that persists at ./${PATH}/${FUNCTION_NAME}_index.pickle'''

    def __init__(self, parent_dir, name, serializer=None):
        Index.__init__(self, name)
        if serializer is None:
            import pickle
            serializer = pickle
        self.serializer = serializer
        self.path = pathify(parent_dir) / (name + '_index.pickle')

        if self.path.exists():
            with self.path.open('rb') as f:
                self.data = self.serializer.load(f)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.data = {}

    def __setitem__(self, key, val):
        super().__setitem__(key, val)
        self.dump()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.dump()

    def dump(self):
        with self.path.open('wb') as f:
            self.serializer.dump(self.data, f)

    def clear(self):
        super().clear()
        if self.path.exists():
            self.path.unlink()


def rand_name(n=10):
    return ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(n))


def unused_file(directory):
    while True:
        fname = directory / rand_name()
        if fname.exists():
            return fname


def hashable(obj):
    '''Converts args and kwargs into a hashable type (overridable)'''
    try:
        hash(obj)
    except:
        if hasattr(obj, 'items'):
            # turn dictionaries into frozenset((key, val))
            return frozenset((key, hashable(val))
                             for key, val in obj.items())
        elif hasattr(obj, '__iter__'):
            # turn iterables into tuples
            return tuple(hashable(val) for val in obj)
        else:
            raise TypeError(f"I don't know how to hash {obj} ({type(obj)})")
    else:
        return obj


def pathify(obj):
    if isinstance(obj, (str, bytes)):
        return Path(obj)
    else:
        return obj


if __name__ == '__main__':
    import shutil
    calls = []

    @Cache.decor(IndexInRam.create(), [NoStore.create()])
    def square1(x):
        calls.append(x)
        return x**2

    @Cache.decor(IndexInFile.create('cache/'), [NoStore.create()])
    def square2(x):
        calls.append(x)
        return x**2

    @Cache.decor(IndexInFile.create('cache/'), [FileStore.create('cache/')])
    def square3(x):
        calls.append(x)
        return x**2

    for square in [square1, square2, square3]:
        calls.clear()
        square.clear()
        square(7) # miss
        square(2) # miss
        square(7)
        square(2)
        square.clear()
        square(7) # miss

        assert calls == [7, 2, 7]

    shutil.rmtree('cache')
