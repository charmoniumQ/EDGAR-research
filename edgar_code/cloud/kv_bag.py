import dask.bag


class KVBag(dask.bag.Bag):
    @classmethod
    def read_text(Class, *args, **kwargs):
        return Class.from_bag(dask.bag.read_text(*args, **kwargs))

    def map_values(self, func):
        return self.map(lambda pair: (pair[0], func(pair[1])))

    def filter_values(self, func):
        return self.filter(lambda pair: func(pair[1]))

    # modify to return subclass

    def filter(self, func):
        return KVBag.from_bag(super().filter(func))

    def map(self, func):
        return KVBag.from_bag(super().map(func))

    # public constructors. use these insead of __init__

    @classmethod
    def from_sequence(Class, seq):
        return Class.from_bag(bag.from_sequence(seq))

    @classmethod
    def from_bag(Class, bag):
        return Class(bag.dask, bag.name, bag.npartitions)

# Cache intermediate steps
# Cascading updates
# Skip precomputed work
