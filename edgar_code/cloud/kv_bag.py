import dask.bag


class KVBag(dask.bag.Bag):
    @classmethod
    def read_text(Class, *args, **kwargs):
        return Class.from_bag(dask.bag.read_text(*args, **kwargs))

    def map_values(self, func):
        def mapper(pair):
            return (pair[0], func(pair[1]))
        mapper.__name__ = f'KVBag.map({func.__name__})'
        return self.map(mapper)

    def filter_values(self, func):
        def filterer(pair):
            return func(pair[1])
        mapper.__name__ = f'KVBag.filter({func.__name__})'
        return self.filter(filterer)

    # modify to return subclass

    def filter(self, func):
        return KVBag.from_bag(super().filter(func))

    def map(self, func):
        return KVBag.from_bag(super().map(func))

    # these return regular ole bags

    def values(self):
        def values_picker(pair):
            return pair[1]
        return super().map(values_picker)

    def keys(self):
        def keys_picker(pair):
            return pair[0]
        return super().map(keys_picker)

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
