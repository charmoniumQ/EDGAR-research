import pathlib
import pickle


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


def cache_to_file(filename, hit_msg=None, miss_msg=None):
    path = pathlib.Path(filename)

    def decorator(func):
        def cached_f(*args, **kwargs):
            cache = {}
            if path.exists():
                with path.open('rb') as f:
                    cache = pickle.load(f)

            key = hashable((args, kwargs))

            if key in cache:
                if hit_msg: print(hit_msg.format(**locals()))
                return cache[key]
            else:
                if miss_msg: print(miss_msg.format(**locals()))
                val = func(*args, **kwargs)
                cache[key] = val
                with path.open('wb+') as f:
                    pickle.dump(cache, f)
                return val

        return cached_f
    return decorator
