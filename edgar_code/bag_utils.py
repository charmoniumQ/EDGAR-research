from typing import TypeVar, Callable, List, Any
from dask.highlevelgraph import HighLevelGraph
from edgar_code.types import Bag


T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


def map_const(
        func: Callable[[T, U], V],
        bag: Bag[T],
        delayed: Any,
) -> Bag[V]:
    # TODO: don't use Any here
    name = f'map_const({func}, {bag.name}, {delayed.key})'
    def map_chunk(partition: List[T], const: U) -> List[V]:
        return [func(item, const) for item in partition]

    dsk = {
        (name, n): (map_chunk, (bag.name, n), delayed.key)
        for n in range(bag.npartitions)
    }

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[bag, delayed])

    return type(bag)(graph, name, bag.npartitions)
