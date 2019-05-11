from typing import cast, TYPE_CHECKING, List
from pathlib import Path
import tempfile
import dask.config
from dask.bag import Bag as _Bag
from edgar_code.cache import Cache
from edgar_code.bag_store import BagStore


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


def test_bag_store() -> None:
    dask.config.set(scheduler='synchronous')
    # this is so that the worker can access calls (shared mem)

    with tempfile.TemporaryDirectory() as work_dir_:
        work_dir = Path(work_dir_)
        calls: List[int] = []

        @Cache.decor(BagStore.create(work_dir))
        def bag_of_squares(n: int, **kwargs: int) -> Bag[int]: # pylint: disable=invalid-name
            calls.append(n)
            return (
                dask.bag.range(n, npartitions=2)
                .map(lambda x: x**2 + kwargs.get('a', 0))
            )

        calls.clear()
        assert bag_of_squares(3).compute() == [0, 1, 4] # miss
        assert bag_of_squares(2).compute() == [0, 1] # miss

        # repeated values should not miss; they will not show up in
        # calls
        assert bag_of_squares(3).compute() == [0, 1, 4]
        assert bag_of_squares(2).compute() == [0, 1]

        # clear the cache, so next should be miss
        cast(Cache, bag_of_squares).clear()
        assert bag_of_squares(3).compute() == [0, 1, 4] # miss

        # adding a kwarg should make a miss
        assert bag_of_squares(3).compute() == [0, 1, 4]
        assert bag_of_squares(3, a=2).compute() == [2, 3, 6] # miss

        # test del explicitly
        # no good way to test it no normal functionality
        cache = cast(Cache, bag_of_squares)
        del cache.obj_store[cache.obj_store.args2key((3,), {})]
        assert bag_of_squares(3).compute() == [0, 1, 4] # miss

        assert calls == [3, 2, 3, 3, 3]
