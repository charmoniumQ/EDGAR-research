from .cache_bag import CacheBag


class KVBag(CacheBag):
    def __init__(self, bag):
        self.bag = bag

    def map_values(self, func):
        return self.map(lambda pair: (pair[0], func(pair[1])))

    def filter_values(self, func):
        return self.filter(lambda pair: func(pair[1]))

    # modify to return subclass

    def filter(self, func):
        return KVBag(self.bag.filter(func))

    def map(self, func):
        return KVBag(self.bag.map(func))

    # public constructors. use thes insead of __init__

    @classmethod
    def from_keys(Cls, bag):
        return Cls(bag.map(lambda key: (key, key)))

    @classmethod
    def from_pairs(Cls, bag):
        return Cls(bag)

    # inherit rest

    def __getattr__(self, attri):
        return getattr(self.bag, attri)

# Cache intermediate steps
# Cascading updates
# Skip precomputed work
