from .fs_util import (
    rand_names,
    sanitize_unused_fname,
    new_directory,
)
from . import cache


def download_retry(url, max_retries=10, cooldown=5):
    import urllib
    import time
    for retry in range(max_retries):
        try:
            return urllib.request.urlopen(url).read()
        except Exception as e:
            if retry == max_retries - 1:
                raise e
            else:
                time.sleep(cooldown)


class Struct(object):
    pass


def generator_to_list(f):
    def f_(*args, **kwargs):
        return list(f(*args, **kwargs))
    return f_


def invert(dct):
    return {value: key for key, value in dct.items()}


def dicts2csr(dicts, width=0):
    from scipy.sparse import csr_matrix
    indptr = [0]
    indices = []
    data = []
    for dct in dicts:
        for ind, val in dct.items():
            width = max(width, ind)
            indices.append(ind)
            data.append(val)
        indptr.append(len(indices))
    return scipy.sparse.csr_matrix((data, indices, indptr), shape)

def generator2iterator(generator, length=None):
    class Iterator(object):
        def __init__(self):
            self.length = length
        def __iter__(self):
            if self.length is not None:
                yield from generator()
            else:
                length = 0
                for e in generator():
                    length += 1
                    yield e
                self.length = length
        def __len__(self):
            if self.length is not None:
                return self.length
            else:
                raise RuntimeError('len not known yet')
    return Iterator()
