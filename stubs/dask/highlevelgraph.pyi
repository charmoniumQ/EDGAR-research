from typing import Hashable, Callable, Dict, Tuple, Any

# https://docs.dask.org/en/latest/spec.html
Key = Hashable

Task = Tuple[Any, ...]
# Task = Tuple[Callable, Computation, ...]

Computation = Any

class HighLevelGraph:
    dicts: Dict[Key, Task]
