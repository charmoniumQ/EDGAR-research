from typing import Hashable, Callable, Dict, Tuple, Any, List

# https://docs.dask.org/en/latest/spec.html
Key = Hashable

Task = Tuple[Any, ...]
# Task = Tuple[Callable, Computation, ...]

Computation = Any

class HighLevelGraph:
    dicts: Dict[Key, Task]
    @classmethod
    def HighLeveLGraph(
            cls,
            name: str,
            dsk: Dict[Key, Task],
            dependencies: List[Any],
    ) -> HighLevelGraph:
        # TODO: don't use Any here
        ...
