import pathlib
import pickle
import glob
import pyspark
from haikunator import Haikunator
from util.cache_to_file import hashable


my_sc = None


def get_sc(name):
    global my_sc
    if my_sc is None:
        name += Haikunator.haikunate(0)
        print(name)
        conf = pyspark.SparkConf().setAppName(name)
        my_sc = pyspark.SparkContext(conf=conf)
        for egg in glob.glob('*.egg'):
            my_sc.addPyFile(egg)
        return my_sc
    else:
        return my_sc


def spark_cache(sc, filename, bucketname, hit_msg=None, miss_msg=None):
    path = pathlib.Path(filename)

    def decorator(func):
        cache = {}
        if path.exists():
            with path.open('rb') as f:
                cache = pickle.load(f)

        def cached_f(*args, **kwargs):
            key = hashable((args, kwargs))

            if key in cache:
                if hit_msg: print(hit_msg.format(**locals())) # noqa
                url = cache[key]
                rdd = sc.pickleFile(url)
            else:
                if miss_msg: print(miss_msg.format(**locals())) # noqa
                rdd = func(*args, **kwargs)
                url = bucketname + Haikunator.haikunate(0, '_')
                rdd.saveAsPickleFile(url)
                cache[key] = url
                with path.open('wb+') as f:
                    pickle.dump(cache, f)
            return rdd

        return cached_f
    return decorator
