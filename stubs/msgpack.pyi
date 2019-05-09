from typing import Optional, Callable, Any

def dump(
        obj: Any,
        f: Any,
        use_bin_type=False,
        default: Optional[Callable[[Any], Any]] = None,
        use_list=False,
        strict_types=False,
) -> None:
    ...
def dumps(
        obj: Any,
        use_bin_type=False,
        default: Optional[Callable[[Any], Any]] = None,
        use_list=False,
        strict_types=False,
) -> bytes:
    ...
def loads(
        b: bytes,
        raw=True,
        object_hook: Optional[Callable[[Any], Any]] = None,
        use_list=False,
) -> Any:
    ...
def load(
        f: Any,
        raw=True,
        object_hook: Optional[Callable[[Any], Any]] = None,
        use_list=False,
) -> Any:
    ...
