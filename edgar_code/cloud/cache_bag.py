import pickle
import random
import string
import toolz
import s3fs
import dask.bag
from ..util.cache import Cache, FileBackedCache


class BagCache(FileBackedCache):
    '''FileBackedCache, specialized for Dask bags'''

    def __init__(self, bucket, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket

    def __setitem__(self, key, obj):
        if isinstance(obj, dask.bag.Bag):
            val = ('bag', dump_bag(self.bucket, obj))
        else:
            val = ('obj', obj)
        super().__setitem__(key, val)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        if val[0] == 'bag':
            parition_urls = val[1]
            obj = load_bag(self.bucket, parition_urls)
        else:
            obj = val[1]
        return obj

    def clear(self):
        fs = s3fs.S3FileSystem()
        for key, val in self.data.items():
            if val[0] == 'bag':
                partition_urls = val[1]
                for partition in partition_urls:
                    fs.rm(partition)
        super().clear()


def dump_bag(bucket, bag):
    fs = s3fs.S3FileSystem()
    partition_urls = (
        bag
        .map_partitions(dump_partition(fs, bucket))
        .compute()
    )
    return partition_urls


@toolz.curry
def dump_partition(fs, bucket, partition):
    name = random_string()
    path = 's3://{bucket}/{name}'.format(**locals())
    with fs.open(path, 'wb') as f:
        pickle.dump(partition, f)
    return path


def load_bag(bucket, partition_urls):
    fs = s3fs.S3FileSystem()
    return (
        dask.bag.from_sequence(partition_urls, partition_size=1)
        .map(load_partition(fs))
        .flatten()
    )


@toolz.curry
def load_partition(fs, path):
    with fs.open(path, 'rb') as f:
        return pickle.load(f)


def random_string(n=20):
    name = ''.join(random.sample(string.ascii_lowercase, n))
    return name


if __name__ == '__main__':
    calls = []

    @Cache(BagCache('jlndhfkiab-edgar-data', 'test'))
    def f(a):
        calls.append(a)
        return dask.bag.range(a, npartitions=2)

    f.clear()
    f(7).compute() # miss
    f(2).compute() # miss
    f(7).compute()
    f(2).compute()
    f.clear()
    f(7).compute() # miss
    f.clear()

    assert calls == [7, 2, 7]
