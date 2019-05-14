from typing import TypeVar, Generic


T = TypeVar('T')

class Future(Generic[T]):
    key: str
