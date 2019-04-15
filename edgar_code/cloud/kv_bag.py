import dask.bag


class KVBag(dask.bag.Bag):
    @classmethod
    def read_text(Class, *args, **kwargs):
        return Class.from_bag(dask.bag.read_text(*args, **kwargs))

    def map_values(self, func):
        def mapper(pair):
            return (pair[0], func(pair[1]))
        mapper.__name__ = f'KVBag.map({func.__qualname__})'
        return self.map(mapper)

    def filter_values(self, func):
        def filterer(pair):
            return func(pair[1])
        filterer.__name__ = f'KVBag.filter({func.__qualname__})'
        return self.filter(filterer)

    def map_keys(self, func):
        def mapper(pair):
            return (func(pair[0]), pair[1])
        mapper.__name__ = f'KVBag.map({func.__qualname__})'
        return self.map(mapper)

    # def enumerate(self):
    #     def tupler(*args):
    #         return args
    #     self.dask.dicts[self.name] = {
    #         key: (tupler, val key[1],)
    #         for key, val in self.dask.dicts[self.name].items()
    #     }
    #     return result

    def flatten_values(self):
        def rekey(pair):
            key, vals = pair
            return [((key, i), val) for i, val in enumerate(vals)]
        return self.map(rekey).flatten()

    # modify to return subclass

    def filter(self, func):
        return KVBag.from_bag(super().filter(func))

    def map(self, func):
        return KVBag.from_bag(super().map(func))

    def flatten(self):
        return KVBag.from_bag(super().flatten())

    # these return regular ole bags

    def values(self):
        def values_picker(pair):
            return pair[1]
        return super().map(values_picker)

    def keys(self):
        def keys_picker(pair):
            return pair[0]
        return super().map(keys_picker)

    @classmethod
    def concat(Class, bags):
        Class.from_bag(dask.bag.concat(bags))

    # public constructors. use these insead of __init__

    @classmethod
    def from_sequence(Class, seq, npartitions=None, partition_size=None):
        return Class.from_bag(dask.bag.from_sequence(seq, npartitions, partition_size))

    @classmethod
    def from_bag(Class, bag):
        return Class(bag.dask, bag.name, bag.npartitions)

# Cache intermediate steps
# Cascading updates
# Skip precomputed work
