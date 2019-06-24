from typing import (
    TypeVar, Callable, List, Iterable, Hashable, Any, Dict, Tuple,
)
from dask.highlevelgraph import HighLevelGraph
from edgar_code.types import Bag, Delayed


T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


def map_const(
        func: Callable[[T, U], V],
        bag: Bag[T],
        delayed: Delayed[U],
) -> Bag[V]:
    name = f'map_const({func}, {bag.name}, {delayed.key})'
    def map_chunk(partition: Iterable[T], const: U) -> List[V]:
        return [func(item, const) for item in partition]

    dsk: Dict[Hashable, Tuple[Any, ...]] = {
        (name, n): (map_chunk, (bag.name, n), delayed.key)
        for n in range(bag.npartitions)
    }

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[bag, delayed])

    return Bag[V](graph, name, bag.npartitions)
