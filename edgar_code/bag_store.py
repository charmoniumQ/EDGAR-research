from typing import (
    Optional, TypeVar, Any, Tuple, Dict, Iterable, List,
    Callable, cast, TYPE_CHECKING
)
import shutil
from dask.bag import Bag as _Bag
import dask.bag
from edgar_code.gs_path import PathLike, is_pathlike
from  edgar_code.cache import Serializer, ObjectStore, safe_str


#### Types ####

# https://stackoverflow.com/a/48554601/1078199
if TYPE_CHECKING:
    # we are running in mypy
    # which understands Bag[T]
    Bag = _Bag
else:
    class FakeGenericMeta(type(_Bag)):
        def __getitem__(cls, item):
            return cls

    # I need to make `Bag` subscriptable
    # subscripting FakeGenericMeta is a no-op
    # so `Bag[T] is Bag`
    class Bag(_Bag, metaclass=FakeGenericMeta):
        pass

T = TypeVar('T')
U = TypeVar('U')


#### Main ####

class BagStore(ObjectStore[PathLike, Bag[T]]):
    def __init__(
            self, bag_path: PathLike, name: str,
            serializer: Optional[Serializer] = None
    ) -> None:
        super().__init__(name)
        if serializer is None:
            import pickle
            self.serializer = cast(Serializer, pickle)
        else:
            self.serializer = serializer
        self.bag_path = bag_path / name

    def args2key(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> PathLike:
        if kwargs:
            args = args + (kwargs,)
        name = safe_str(args)
        return self.bag_path / name

    def __setitem__(self, bag_path: PathLike, bag: Bag[T]) -> None:
        # I am hacking map_partitions to also take a partition_no
        new_bag = cast(Any, bag).map_partitions(self.make_dump_partition(bag_path))
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
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with index_path.open('wb') as fil:
            self.serializer.dump((bag.npartitions, type(bag)), fil)

    def make_dump_partition(
            self, bag_path: PathLike
    ) -> Callable[[Iterable[T], int], List[T]]:
        def dump_partition(partition: Iterable[T], partition_no: int) -> List[T]:
            partition_list = list(partition)
            partition_path = bag_path / f'part_{partition_no}.pickle'
            with partition_path.open('wb') as fil:
                self.serializer.dump(partition_list, fil)
            # return partition so that it is transparent to the rest of the task graph
            return partition_list
        return dump_partition

    def __getitem__(self, bag_path: PathLike) -> Bag[T]:
        index_path = bag_path / 'index.pickle'
        with index_path.open('rb') as fil:
            npartitions, bag_type = self.serializer.load(fil)
        bag_constructor: Callable[[Bag[T]], Bag[T]] = (
            (lambda x: x) if bag_type == dask.bag.Bag else bag_type
        )

        return bag_constructor(
            dask.bag.range(npartitions, npartitions=npartitions)
            .map_partitions(self.make_load_partition(bag_path))
        )

    def make_load_partition(
            self, bag_path: PathLike
    ) -> Callable[[List[int]], List[T]]:
        def load_partition(partition_no_list: List[int]) -> List[T]:
            partition_no = partition_no_list[0]
            partition_path = bag_path / f'part_{partition_no}.pickle'
            with partition_path.open('rb') as fil:
                return cast(List[T], self.serializer.load(fil))
        return load_partition

    def __contains__(self, bag_path: Any) -> bool:
        if is_pathlike(bag_path):
            index_path = bag_path / 'index.pickle'
            if index_path.exists():
                with index_path.open('rb') as fil:
                    npartitions, _ = self.serializer.load(fil)
                if len(list(bag_path.iterdir())) == npartitions + 1:
                    return True
                else:
                    # We have a partially stored bag
                    # Not valid, so delete before anyone gets confused
                    del self[bag_path]
                    return False
            else:
                return False
        else:
            return False

    def __delitem__(self, bag_path: PathLike) -> None:
        if hasattr(bag_path, 'rmtree'):
            cast(Any, bag_path).rmtree()
        else:
            shutil.rmtree(str(bag_path))

    def clear(self) -> None:
        if hasattr(self.bag_path, 'rmtree'):
            cast(Any, self.bag_path).rmtree()
        else:
            shutil.rmtree(str(self.bag_path))
