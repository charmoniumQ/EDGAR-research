import collections
import pathlib
import pickle
import os


class Cache(object):
    def __init__(self, dct, hit_msg=None, miss_msg=None, suffix=''):
        '''
        dct must satisfy:
            __contains__(key: hashable) -> bool
            __setitem__(key: hashable, val: any)
            __getitem__(key: hashable) -> any
            clear()
            load(name: str) # initializes the state of the dct

        '{key}' in hit_msg and miss_msg will be replaced with the caller's args
        and kwargs

        Note this uses function.__name__ to determine the file name.
        If this is not unique within your program, define suffix in ex'''
        self.dct = dct
        self.hit_msg = hit_msg
        self.miss_msg = miss_msg
        self.suffix = suffix

    def __call__(self, *args, **kwargs):
        if hasattr(self, 'function'):
            return self.cached_function(*args, **kwargs)
        else:
            return self.set_function(*args, **kwargs)

    def set_function(self, function):
        '''Gets called when the decarotor is applied'''
        self.function = function
        name = function.__name__ + self.suffix
        self.dct.load(name)
        return self

    def cached_function(self, *args, **kwargs):
        '''gets called where the funciton would be called'''
        key = self.hashable((args, kwargs))
        if key in self.dct:
            res = self.dct[key]
            if self.hit_msg:
                print(self.hit_msg.replace('{key}', repr((args, kwargs))))
        else:
            res = self.function(*args, **kwargs)
            self.dct[key] = res
            if self.miss_msg:
                print(self.miss_msg.replace('{key}', repr((args, kwargs))))
        return res

    @classmethod
    def hashable(Cls, obj):
        '''Converts args and kwargs into a hashable type (overridable)'''
        try:
            hash(obj)
        except:
            if hasattr(obj, 'items'):
                # turn dictionaries into frozenset((key, val))
                return frozenset((key, Cls.hashable(val))
                                 for key, val in obj.items())
            elif hasattr(obj, '__iter__'):
                # turn iterables into tuples
                return tuple(Cls.hashable(val) for val in obj)
            else:
                raise TypeError("I don't know how to hash {obj} ({type})"
                                .format(type=type(obj), obj=obj))
        else:
            return obj

    def clear(self):
        self.dct.clear()

    def __repr__(self):
        dct_type = type(self.dct).__name__
        if hasattr(self, 'function'):
            return 'Cache of {self.function!r} with {dct_type}' \
                .format(**locals())
        else:
            return 'Unapplied decorator with {dct_type}' \
                .format(**locals())


class DictBackedCache(collections.UserDict):
    # self.data references the real internal dict
    def load(self, name):
        pass


class FileBackedCache(DictBackedCache):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = pathlib.Path(path)

    def load(self, name):
        self.filename = self.path / name

        if self.filename.exists():
            with self.filename.open('rb') as f:
                self.data = pickle.load(f)
        else:
            self.data = {}

    def __setitem__(self, *args, **kwargs):
        super().__setitem__(*args, **kwargs)
        if not self.filename.parent.exists():
            self.filename.parent.mkdir(parents=True)
            # TODO: new in Python3.5, exist_ok=True
        with self.filename.open('wb') as f:
            pickle.dump(self.data, f)

    def clear(self):
        super().clear()
        if self.filename.exists():
            os.remove(str(self.filename))


if __name__ == '__main__':
    calls = []

    @Cache(DictBackedCache())
    def square1(x):
        calls.append(x)
        return x**2

    @Cache(FileBackedCache('test'))
    def square2(x):
        calls.append(x)
        return x**2

    for square in [square1, square2]:
        calls.clear()
        square.clear()
        square(7) # miss
        square(2) # miss
        square(7)
        square(2)
        square.clear()
        square(7) # miss

        assert calls == [7, 2, 7]
