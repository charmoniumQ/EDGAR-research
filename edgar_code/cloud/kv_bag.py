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
    #         key: (tupler, i, val)
    #         for i, (key, val) in enumerate(self.dask.dicts[self.name].items())
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

    @classmethod
    def concat(Class, bags):
        bag = dask.bag.concat(bags)
        return Class(bag.dask, bag.name, bag.npartitions)

import dask.bag
import dask.highlevelgraph
def map_const(func, bag, delayed):
    name = f'map_const({func}, {bag.name}, {delayed.key})'
    def map_chunk(partition, const):
        return [func(item, const) for item in partition]

    dsk = {
        (name, n): (map_chunk, (bag.name, n), delayed.key)
        for n in range(bag.npartitions)
    }

    graph = dask.highlevelgraph.HighLevelGraph.from_collections(name, dsk, dependencies=[bag, delayed])

    return type(bag)(graph, name, bag.npartitions)

# test
# runs = []
# def side_effect_func(a):
#     runs.append(a)
#     return a + a

# n = 30
# bag = dask.bag.range(n, npartitions=10)
# delayed = dask.delayed(side_effect_func)('foo')
# lst = map_const(lambda a, b: f'{a} {b}', bag, delayed).compute()
# print(lst == [f'{i} foofoo' for i in range(n)])

# Cache intermediate steps
# Cascading updates
# Skip precomputed work
