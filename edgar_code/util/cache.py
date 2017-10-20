import collections
from pathlib import Path, PurePath
import pickle
import os
import s3fs as s3fs_client
from edgar_code.util import rand_name


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
        self.name = function.__name__ + self.suffix
        self.dct.load(self.name)
        return self

    def cached_function(self, *args, **kwargs):
        '''gets called where the funciton would be called'''
        key = self.hashable_args(*args, **kwargs)
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
    def hashable_args(Cls, *args, **kwargs):
        return Cls.hashable((args, kwargs))

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

    def clear_item(self, *args, **kwargs):
        key = self.hashable_args(*args, **kwargs)
        del self.dct[key]

    def __repr__(self):
        dct_type = type(self.dct).__name__
        if hasattr(self, 'function'):
            return 'Cache of {self.function!r} with {dct_type}' \
                .format(**locals())
        else:
            return 'Unapplied decorator with {dct_type}' \
                .format(**locals())

class IndexedCache(Cache):
    '''Indexed cache

Stores index separately from objects.'''

    def __init__(self, store, *args, **kwargs):
        '''Store must implement load(obj) -> address and dump(address) -> obj where
address is picklable'''
        super().__init__(*args, **kwargs)
        self.store = store

    def cached_function(self, *args, **kwargs):
        '''gets called where the funciton would be called'''
        key = self.hashable_args(*args, **kwargs)
        if key in self.dct:
            res = self.store.load(self.dct[key])
            if self.hit_msg:
                print(self.hit_msg.replace('{key}', repr((args, kwargs))))
        else:
            res = self.function(*args, **kwargs)
            self.dct[key] = self.store.dump(res)
            if self.miss_msg:
                print(self.miss_msg.replace('{key}', repr((args, kwargs))))
        return res

    def clear(self):
        for key in self.dct:
            self.store.clear_item(self.dct[key])
        super().clear()

    def clear_item(self, *args, **kwargs):
        key = self.hashable_args(*args, **kwargs)
        self.store.clear(self.dct[key])
        super().clear_item(key)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self.store.name = name
        self._name = name


class FileStore(object):
    def __init__(self, path):
        self.path = path
        self.path.mkdir(parents=True)
        self._name = 'none'

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name
        self.path = self.path / name

    def load(self, address):
        print(repr(self.path), repr(address))
        filename = self.path / address
        print(filename)
        with filename.open('rb') as f:
            return pickle.load(f)

    def dump(self, obj):
        while True:
            filename = self.path / rand_name(10)
            if not filename.exists():
                break
        with filename.open('wb') as f:
            pickle.dump(obj, f)
        return filename

    def clear_item(self, address):
        if hasattr(address, 'remove'):
            address.remove()
        else:
            os.remove(addresss)


class DictInRam(collections.UserDict):
    # self.data references the real internal dict
    def load(self, name):
        pass


class DictInFile(DictInRam):
    def __init__(self, path):
        if type(path) is str:
            self.path = Path(path)
        else:
            self.path = path

    def __setitem__(self, key, val):
        super().__setitem__(key, val)
        self.dump()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.dump()

    def load(self, name):
        self.filename = self.path / name

        if self.filename.exists():
            with self.filename.open('rb') as f:
                self.data = pickle.load(f)
        else:
            self.data = {}

    def dump(self):
        if not self.filename.parent.exists():
            self.filename.parent.mkdir(parents=True)
            # TODO: new in Python3.5, exist_ok=True
        with self.filename.open('wb') as f:
            pickle.dump(self.data, f)

    def clear(self):
        super().clear()
        if self.filename.exists():
            if hasattr(self.filename, 'remove'):
                self.filename.remove()
            else:
                os.remove(str(self.filename))


class S3Path(object):
    def __init__(self, bucket, path, s3fs):
        self.s3fs = s3fs
        self.bucket = bucket
        self.path = PurePath(path)
        if self.path.anchor != '/':
            self.path = '/' / self.path

    @property
    def bucket_path(self):
        return self.bucket + str(self.path)

    def exists(self):
        return self.s3fs.exists(self.bucket_path)

    def open(self, mode):
        if mode not in ['rb', 'wb']:
            raise ValueError('{mode} is not supported for s3fs paths'.format(**locals()))
        return self.s3fs.open(self.bucket_path, mode)

    @property
    def parent(self):
        return S3Path(self.bucket, self.path.parent, self.s3fs)

    def __truediv__(self, subdir):
        return S3Path(self.bucket, self.path / subdir, self.s3fs)

    def mkdir(self, parents=False):
        # s3 keystore does not require the 'parent directory' key to exist first
        # self.s3fs.mkdir(self.bucket_path)
        pass

    def remove(self):
        self.s3fs.rm(self.bucket_path)

    def __repr__(self):
        return 's3://{self.bucket}{self.path}'.format(**locals())

if __name__ == '__main__':
    import yaml

    with Path('/home/sam/Box Sync/config.yaml').open('r') as f:
        config = yaml.load(f)

    with Path('/home/sam/Box Sync/credentials.yaml').open('r') as f:
        credentials = yaml.load(f)

    s3fs = s3fs_client.S3FileSystem(
        key=credentials['aws']['cache']['access_key_id'],
        secret=credentials['aws']['cache']['secret_access_key'],
    )
    bucket = config['bucket']

    calls = []

    @Cache(DictInRam())
    def square1(x):
        calls.append(x)
        return x**2

    @Cache(DictInFile('cache'))
    def square2(x):
        calls.append(x)
        return x**2

    @Cache(DictInFile(S3Path(bucket, '/cache1', s3fs)))
    def square3(x):
        calls.append(x)
        return x**2

    @IndexedCache(FileStore(S3Path(bucket, '/cache2', s3fs)), DictInFile(S3Path(bucket, '/cache2', s3fs)))
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
