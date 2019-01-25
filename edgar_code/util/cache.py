import collections
from pathlib import Path
import shutil
import toolz
import pickle
import os
from .fs_util import rand_names


# TODO: allow caching 'named objects'. Cache the name instead of the object.
# TODO: allow caching objects by provenance. Cache the thing you did to make the object instead of the object.
# https://github.com/bmabey/provenance

class Cache(object):
    def __init__(self, index, store, hit_msg=None, miss_msg=None, suffix=''):
        '''
        index must satisfy:
            __contains__(key: hashable) -> bool
            __setitem__(key: hashable, val: any)
            __getitem__(key: hashable) -> any
            __deltitem__(key: hashable)
            clear()
            set_name(name: str) # initializes the state of the index

        store must satisfy:
            put(obj: any) -> your_key_type
            get(key: your_key_type) -> any
            remove(key: your_key_type)
            set_name(name: str)
            clear() # optional

        The index is a map from function_arguments -> key
        The store is a map from key -> function_returned_object
        where the key is determined entirely by the store.

        This lets us have indirection: the index to the cache is stored
        separately from the (potentially large) objects in the cache.

        If you do not need this indirection, just use NoStore() as the store.

        '{key}' in hit_msg and miss_msg will be replaced with the caller's args
        and kwargs, while '{name}' gets replaced by the cache name

        Note this uses function.__name__ to determine the file name.
        If this is not unique within your program, define suffix'''
        self.index = index
        self.store = store
        self.hit_msg = hit_msg
        self.miss_msg = miss_msg
        self.suffix = suffix

    def __call__(self, *args, **kwargs):
        if hasattr(self, 'function'):
            return self.cached_function(*args, **kwargs)
        else:
            return self.set_function(*args, **kwargs)

    def set_function(self, function):
        '''Gets called when the decorator is applied'''
        self.function = function
        self.name = function.__name__ + self.suffix
        self.index.set_name(self.name)
        self.store.set_name(self.name)
        return self

    def cached_function(self, *args, **kwargs):
        '''gets called where the funciton would be called'''
        key = self.hashable_args(*args, **kwargs)
        if key in self.index:
            if self.hit_msg:
                print(self.hit_msg
                      .format(key=repr((args, kwargs)), name=self.name))
            res = self.store.get(self.index[key])
        else:
            if self.miss_msg:
                print(self.miss_msg
                      .format(key=repr((args, kwargs)), name=self.name))
            res = self.function(*args, **kwargs)
            self.index[key] = self.store.put(res)
        return res

    @classmethod
    def hashable_args(Cls, *args, **kwargs):
        return hashable((args, kwargs))

    def clear(self):
        if hasattr(self.store, 'clear'):
            self.store.clear()
        else:
            for key in self.index:
                self.store.remove(key)
        self.index.clear()

    def clear_item(self, *args, **kwargs):
        key = self.hashable_args(*args, **kwargs)
        if key in self.index:
            self.store.remove(key)
            del self.index[key]

    def __repr__(self):
        index_type = type(self.index).__name__
        if hasattr(self, 'function'):
            return 'Cache of {self.name} with {index_type}' \
                .format(**locals())
        else:
            return 'Unapplied decorator with {index_type}' \
                .format(**locals())


class NoStore(object):
    '''Uses the objects as keys.

    This has the effect that the index will end up storing the objects.'''

    def set_name(self, name):
        self.name = name

    def put(self, obj):
        return obj

    def get(self, key):
        return key

    def remove(self, key):
        pass

    def clear(self):
        pass


class PickleStore(object):
    '''Use pickle to store the objects each as its own file in this directory.

    Good for large objects.
    Stores at ./${PARENT_DIR}/${FUNCTION_NAME}/${RAND_STRING}'''

    def __init__(self, parent_dir, gen_key=None):
        self.parent_dir_ = pathify(parent_dir)
        if gen_key is None:
            self.gen_key = toolz.partial(rand_names, 20)
        else:
            self.gen_key = gen_key

    def set_name(self, name):
        self.name = name
        self.dir_ = self.parent_dir_ / name

    def get(self, key):
        with (self.dir_ / key).open('rb') as f:
            return pickle.load(f)

    def get_unused_key(self):
        for key in self.gen_key():
            if not (self.dir_ / key).exists():
                return key

    def put(self, obj):
        if not self.dir_.exists():
            self.dir_.mkdir(parents=True, exist_ok=True)
        key = self.get_unused_key()
        with (self.dir_ / key).open('wb') as f:
            pickle.dump(obj, f)
        return key

    def remove(self, key):
        fname = self.dir_ / key
        if fname.exists():
            if hasattr(fname, 'remove'):
                # to make S3Paths work here
                fname.remove()
            else:
                os.remove(str(fname))

    def clear(self):
        if self.dir_.exists():
            if hasattr(self.dir_, 'remove'):
                # to make S3Paths work here
                self.dir_.remove()
            else:
                shutil.rmtree(str(self.dir_))


class CustomStore(PickleStore):
    '''Like PickleStore, but overridable.

    Classes wishing to use this should provide instance-method put and
    class-method get. These will be called with the args and kwargs in
    this class's __init__.'''

    def __init__(self, path, gen_key=None, **kwargs):
        super().__init__(path, gen_key)
        self.kwargs = kwargs

    def put(self, obj):
        if hasattr(obj, 'put'):
            # the key[0] will be type(obj), so that we can call
            # key[0].get later
            key = (type(obj), obj.put(**self.kwargs))
        else:
            # fall back to PickleStore
            # the key[0] will indicate be None if we used pickle
            key = (None, super().put(obj))
        return key

    def get(self, key):
        if key[0] is not None:
            # note that get  should be a classmethod
            return key[0].get(key[1], **self.kwargs)
        else:
            return super().get(key[1])


class IndexInRam(collections.UserDict):
    '''An index with no persistence'''

    # self.data references the real internal dict
    def set_name(self, name):
        self.name = name


class IndexInFile(collections.UserDict):
    '''An index that persists at ./${DIR_}/${FUNCTION_NAME}_index'''

    def __init__(self, dir_):
        self.dir_ = pathify(dir_)

    def __setitem__(self, key, val):
        super().__setitem__(key, val)
        self.dump()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.dump()

    def set_name(self, name):
        self.fname = self.dir_ / (name + '_index')

        if self.fname.exists():
            with self.fname.open('rb') as f:
                self.data = pickle.load(f)
        else:
            self.data = {}

    def dump(self):
        self.dir_.mkdir(parents=True, exist_ok=True)
        with self.fname.open('wb') as f:
            pickle.dump(self.data, f)

    def clear(self):
        super().clear()
        if self.fname.exists():
            if hasattr(self.fname, 'remove'):
                # to make S3Paths work here
                self.fname.remove()
            else:
                os.remove(str(self.fname))


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
            raise TypeError("I don't know how to hash {obj} ({type})"
                            .format(type=type(obj), obj=obj))
    else:
        return obj


def pathify(obj):
    if type(obj) is str:
        return Path(obj)
    else:
        return obj


if __name__ == '__main__':
    calls = []

    @Cache(IndexInRam(), NoStore())
    def square1(x):
        calls.append(x)
        return x**2

    @Cache(IndexInFile('cache/'), NoStore())
    def square2(x):
        calls.append(x)
        return x**2

    @Cache(IndexInFile('cache2/'), CustomStore('cache2/', None))
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
    shutil.rmtree('cache2')
