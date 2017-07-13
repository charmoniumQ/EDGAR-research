import s3fs
import pickle
import dask.bag
import random
import string

fs = s3fs.S3FileSystem()
bucket = 'jlndhfkiab-edgar-data'
path_prefix = 's3://{bucket}/'.format(**locals())


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


def cache_to_s3(hit_msg=None, miss_msg=None, ex=''):
    '''
    The arguments to the function must all be acceptable arguments where:
    The acceptable objects are  any hashable objects, dictionaries of
    acceptable objects, and iterables of acceptable objects.

    Note this usess function.__name__ to determine the file name.
    If this is not unique within your program, define suffix in ex'''

    def decorator(func):
        filename = bucket + '/' + func.__name__ + ex
        cache = {}
        if fs.exists(filename):
            with fs.open(filename, 'rb') as f:
                cache = pickle.load(f)

        def cached_f(*args, **kwargs):

            key = hashable((args, kwargs))

            if key in cache:
                if hit_msg:
                    print(hit_msg.format(**locals()))
                return cache[key]

            else:
                if miss_msg:
                    print(miss_msg.format(**locals()))
                val = func(*args, **kwargs)
                cache[key] = val

                with fs.open(filename, 'wb') as f:
                    pickle.dump(cache, f)
                return val

        return cached_f

    return decorator


def random_path():
    name = ''.join(random.sample(string.ascii_lowercase, 20))
    return path_prefix + name


def cache_partition(partition):
    path = random_path()
    with fs.open(path, 'wb') as f:
        pickle.dump(partition, f)
    return path


def uncache_partition(path):
    path = path.strip()
    with fs.open(path, 'rb') as f:
        return pickle.load(f)


def uncache_partitions(partition_urls):
    return (
        dask.bag.from_sequence(partition_urls)
        .map(uncache_partition)
        .flatten()
    )


def cache_partitions(bag):
    partition_urls = (
        bag
        .map_partitions(cache_partition)
        .compute()
    )
    return partition_urls


def cache_bag(hit_msg=None, miss_msg=None, suffix=''):
    '''
    The arguments to the function must all be acceptable arguments where:
    The acceptable objects are  any hashable objects, dictionaries of
    acceptable objects, and iterables of acceptable objects.

    Note this usess function.__name__ to determine the file name.
    If this is not unique within your program, define a unique suffix'''

    def decorator(func):
        path = path_prefix + func.__name__ + suffix
        cache = {}
        if fs.exists(path):
            with fs.open(path, 'rb') as f:
                cache = pickle.load(f)

        def cached_f(*args, **kwargs):

            key = hashable((args, kwargs))

            if key in cache:
                if hit_msg:
                    print(hit_msg.format(**locals()))
                partitions_url = cache[key]
                return uncache_partitions(partitions_url)

            else:
                if miss_msg:
                    print(miss_msg.format(**locals()))
                bag = func(*args, **kwargs)
                cache[key] = cache_partitions(bag)

                with fs.open(path, 'wb') as f:
                    pickle.dump(cache, f)

                return bag

        return cached_f

    return decorator


if __name__ == '__main__':
    @cache_bag('hit {key}', 'miss {key}')
    def test_bag(i):
        print('computing', i)
        return dask.bag.range(10, 3)

    i = random.randint(0, 10000)
    test_bag(i)
    test_bag(i)
