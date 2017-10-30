import sys
sys.path.insert(0, '/home/sam/Documents/src/dask')
import dask.bag
import pickle
from ..util import rand_names
import toolz
import base64


class CacheBag(object):
    def __init__(self, bag):
        self.bag = bag

    def put(self, dir_):
        for key in rand_names(20):
            path = dir_ / key
            if not path.exists():
                break
        path = str(path / '*.json.gz')
        (
            self.bag
            .map(pickle.dumps)
            .map(toolz.partial(base64.a85encode, foldspaces=True, wrapcol=0))
            .to_textfiles(path, encoding=None, storage_options=dir_.storage_options()))
        return path

    @classmethod
    def get(Cls, path, dir_):
        return Cls(
            dask.bag.read_text(path, encoding=None, storage_options=dir_.storage_options())
            .map(toolz.partial(base64.a85decode, foldspaces=True))
            .map(pickle.loads)
        )

    def __getattr__(self, attr):
        f = getattr(self.bag, attr)
        if callable(f):
            return lambda *args, **kwargs: CacheBag(f(*args, **kwargs))
        else:
            return f
