from typing import Optional, Callable, Any

def dump(
        obj: Any,
        f: Any,
        use_bin_type: bool = ...,
        default: Optional[Callable[[Any], Any]] = ...,
        use_list: bool = ...,
        strict_types: bool = ...,
) -> None:
    ...
def dumps(
        obj: Any,
        use_bin_type: bool = ...,
        default: Optional[Callable[[Any], Any]] = ...,
        use_list: bool = ...,
        strict_types: bool = ...,
) -> bytes:
    ...
def loads(
        b: bytes,
        raw=True,
        object_hook: Optional[Callable[[Any], Any]] = ...,
        use_list: bool = ...,
) -> Any:
    ...
def load(
        f: Any,
        raw=True,
        object_hook: Optional[Callable[[Any], Any]] = ...,
        use_list: bool = ...,
) -> Any:
    ...
