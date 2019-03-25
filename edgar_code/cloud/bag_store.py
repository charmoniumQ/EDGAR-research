import edgar_code.util.cache as cache

import base64
import pickle
import toolz
import random
import string


# TODO: put this in its own module
# TODO: make sure this stores partitions in gs, returns URLs, which are stored in gs in the index. NOT in an obj store and the index.
class BagStore(cache.Store):
    def __init__(self, parent_path, name, serializer=None):
        super().__init__(name)
        self.parent_path = parent_path
        self.serializer = serializer if serializer is None else pickle

    def can_store(self, obj):
        return hasattr(obj, 'map_partitions')

    def put(self, obj):
        while True:
            key = rand_name()
            bag_path = self.parent_path / key
            if not bag_path.exists():
                break
        items_path = (
            bag
            .map_partitions(toolz.partial(self.dump_partition, bag_path=bag_path))
            .compute()
        )
        bag_type = type(bag)
        return items_path, bag_type

    def dump_partition(self, partition, bag_path):
        partition_path = bag_path / rand_name(20) # pray for no collisions
        with partition_path.open('wb') as f:
            print(partition)
            self.serializer.dump(partition, f)
        return partition_path

    def __getitem__(self, key):
        item_paths, bag_type = key
        return (
            bag_type.from_sequence(item_paths, npartitions=len(items_path))
            .map_partitions(self.load_partition)
        )

    def load_partition(self, item_paths):
        for item_path in item_paths:
            partition = []
            with item_path.open('rb') as f:
                partition.extend(self.serializer.load(f))
            print(partition)
        return partition

    def __delitem__(self, key):
        item_paths, bag_type = key
        for item_path in items_paths:
            item_path.unlink()

    def clear(self):
        pass
