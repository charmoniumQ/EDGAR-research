import os
import dask
import urllib.parse
import edgar_code.util.cache as cache
import toolz
import shutil


class BagStore(cache.ObjectStore):
    def __init__(self, bag_path, name, serializer=None):
        super().__init__(name)
        if serializer is None:
            import pickle
            serializer = pickle
        self.serializer = serializer
        self.bag_path = bag_path / name

    def to_key(self, args, kwargs):
        if kwargs:
            args = args + (kwargs,)
        try:
            name = cache.safe_name(args)
        except TypeError:
            name = str(hash(hashable(args)))
        return self.bag_path / name

    def __setitem__(self, bag_path, bag):
        new_bag = bag.map_partitions((self.dump_partition(bag_path)))
        # get partition number as second argument
        new_bag.dask.dicts[new_bag.name] = {
            key: val + (key[1],)
            for key, val in new_bag.dask.dicts[new_bag.name].items()
        }

        # mutate bag to be new_bag
        bag.npartitions = new_bag.npartitions
        bag.dask = new_bag.dask
        bag.name = new_bag.name

        index_path = bag_path / 'index.pickle'
        with index_path.open('wb') as f:
            self.serializer.dump((bag.npartitions, type(bag)), f)

    @toolz.curry
    def dump_partition(self, bag_path, partition, partition_no):
        partition = list(partition)
        partition_path = bag_path / f'part_{partition_no}.pickle'
        with partition_path.open('wb') as f:
            self.serializer.dump(partition, f)
        # return partition so that it is transparent to the rest of the task graph
        return partition

    def __getitem__(self, bag_path):
        index_path = bag_path / 'index.pickle'
        with index_path.open('rb') as f:
            npartitions, bag_type = self.serializer.load(f)
        if bag_type == dask.bag.Bag:
            bag_type = dask.bag

        return bag_type.from_bag(
            dask.bag.range(npartitions, npartitions=npartitions)
            .map_partitions(self.load_partition(bag_path))
        )

    @toolz.curry
    def load_partition(self, bag_path, partition_no_list):
        partition_no = partition_no_list[0]
        partition_path = bag_path / f'part_{partition_no}.pickle'
        with partition_path.open('rb') as f:
            return self.serializer.load(f)

    def __contains__(self, bag_path):
        index_path = bag_path / 'index.pickle'
        if index_path.exists():
            with index_path.open('rb') as f:
                npartitions, bag_type = self.serializer.load(f)
            if len(list(bag_path.iterdir())) == npartitions + 1:
                return True
            else:
                # We have a partially stored bag
                # Not valid, so delete before anyone gets confused
                del self[bag_path]
                return False
        else:
            return False

    def __delitem__(self, bag_path):
        if hasattr(bag_path, 'rmtree'):
            bag_path.rmtree()
        else:
            shutil.rmtree(bag_path)

    def clear(self):
        if hasattr(self.bag_path, 'rmtree'):
            self.bag_path.rmtree()
        else:
            shutil.rmtree(self.bag_path)
