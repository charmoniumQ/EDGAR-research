import pathlib
import pickle
import glob
import pyspark
from haikunator import Haikunator
from util.cache_to_file import hashable


sc_ = None


def sc():
    return sc_


def make_sc(name, default_sc=None):
    global sc_
    if sc_ is None:
        if default_sc is not None:
            sc_ = default_sc
        else:
            name += '_' + Haikunator.haikunate(0, '_')
            print('App name: {name}'.format(**locals()))
            conf = pyspark.SparkConf().setAppName(name)
            sc_ = pyspark.SparkContext(conf=conf)
            for egg in glob.glob('*.egg'):
                sc_.addPyFile(egg)


def saveRDD(file, rdd):
    rdd.saveAsPickleFile(str(file))


def loadRDD(file):
    return sc().pickleFile(str(file))


def spark_cache(dest, cdest_, hit_msg='cache hit: {name} {key} {url}', miss_msg='cache miss: {name} {key} {url}'):
    def decorator(func):
        cdest = pathlib.Path(cdest_)
        path = cdest / (func.__name__ + '.pickle')
        cache = {}
        if path.exists():
            with path.open('rb') as f:
                cache = pickle.load(f)

        def cached_f(*args, **kwargs):
            key = hashable((args, kwargs))
            name = func.__name__

            if key in cache:
                url = cache[key]
                if hit_msg: print(hit_msg.format(**locals())) # noqa
                rdd = loadRDD(url)
            else:
                rdd = func(*args, **kwargs)
                url = dest + '/' + Haikunator.haikunate(0, '_')
                if miss_msg: print(miss_msg.format(**locals())) # noqa
                saveRDD(url, rdd)
                cache[key] = url
                with path.open('wb+') as f:
                    pickle.dump(cache, f)
            return rdd

        return cached_f
    return decorator
