class Cache(object):
    def __init__(self, hit_msg=None, miss_msg=None, suffix='', **kwargs):
        '''
        The arguments to the function must all be acceptable arguments where:
        The acceptable objects are  any hashable objects, dictionaries of
        acceptable objects, and iterables of acceptable objects.

        Note this usess function.__name__ to determine the file name.
        If this is not unique within your program, define suffix in ex'''
        self.suffix = suffix
        self.hit_msg = hit_msg
        self.miss_msg = miss_msg
        self.kwargs = kwargs

    def __call__(self, function):
        self.function = function
        self.name = function.__name__ + self.suffix
        self.real_init()
        self.load()

        return CachedF(self)

    def hashable_args(*args, **kwargs):
        return hashable((args, kwargs))

    def real_init(self):
        pass

    def load(self):
        raise NotImplementedError()

    def dump(self):
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    def __repr__(self):
        return '{self.__name__} at {self.name}'.format(**locals())


class CachedF(object):
    def __init__(self, cache):
        self.cache = cache

    def __call__(self, *args, **kwargs):
        key = self.cache.hashable_args(args, kwargs)
        if key in self.cache.cache:
            res = self.cache.cache[key]
            if self.cache.hit_msg:
                print(self.cache.hit_msg.format(**locals()))
        else:
            res = self.cache.function(*args, **kwargs)
            self.cache.cache[key] = res
            self.cache.dump()
            if self.cache.miss_msg:
                print(self.cache.miss_msg.format(**locals()))
        return res

    def clear(self):
        self.cache.clear()


class CacheRam(Cache):
    def load(self):
        self.cache = {}

    def dump(self):
        pass

    def clear(self):
        self.cache = {}

import pathlib
import pickle
import os
class CacheFile(Cache):
    def real_init(self):
        path = pathlib.Path(self.kwargs['path'])
        self.filename = path / self.name

    def load(self):
        if self.filename.exists():
            with self.filename.open('rb') as f:
                self.cache = pickle.load(f)
        else:
            self.cache = {}

    def dump(self):
        if not self.filename.parent.exists():
            self.filename.parent.mkdir(parents=True)
            # TODO: new in Python3.5, exist_ok=True
        with self.filename.open('wb') as f:
            pickle.dump(self.cache, f)

    def clear(self):
        self.cache = {}
        os.remove(self.filename)


import s3fs
import pickle
class CacheS3(Cache):
    def real_init(self):
        bucket = self.kwargs['bucket']
        self.filename = 's3://{bucket}/{self.name}'.format(**locals())
        self.fs = s3fs.S3FileSystem()

    def load(self):
        if self.fs.exists(self.filename):
            with self.fs.open(self.filename, 'rb') as f:
                self.cache = pickle.load(f)
        else:
            self.cache = {}

    def dump(self):
        with self.fs.open(self.filename, 'wb') as f:
            pickle.dump(self.cache, f)

    def clear(self):
        self.cache = {}
        self.fs.rm(self.filename)


def hashable(obj):
    try:
        hash(obj)
    except:
        if hasattr(obj, 'items'):
            return frozenset((key, hashable(val)) for key, val in obj.items())
        elif hasattr(obj, '__iter__'):
            return tuple(hashable(val) for val in obj)
        else:
            raise TypeError("I don't know how to hash {obj} ({type})"
                            .format(type=type(obj), obj=obj))
    else:
        return obj


if __name__ == '__main__':
    calls = []

    @CacheRam()
    def square(x):
        calls.append(x)
        return x**2

    square(7) # miss
    square(2) # miss
    square(7)
    square(2)

    square.clear()
    square(7) # miss

    assert calls == [7, 2, 7]
