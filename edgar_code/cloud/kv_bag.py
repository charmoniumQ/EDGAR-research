import dask.bag


class KVBag(dask.bag.Bag):
    def __init__(self, bag):
        self.bag = bag

    def key_to_value(self, func):
        return self.map(lambda key: (key, func(key)))

    def map_values(self, func):
        return self.map(lambda pair: (pair[0], func(pair[1])))

    def filter_values(self, func):
        return self.filter(lambda pair: func(pair[1]))

    # modify to return subclass

    def filter(self, func):
        return KVBag(self.bag.filter(func))

    def map(self, func):
        return KVBag(self.bag.map(func))

    # inherit rest

    def __getattr__(self, attri):
        return getattr(self.bag, attri)
