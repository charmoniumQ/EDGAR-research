from typing import TypeVar, Generic, Optional, Callable, Any


T = TypeVar('T')
class Delayed(Generic[T]):
    key: str
    def compute(self) -> T:
        ...


#ReturnType = TypeVar('ReturnType')
ReturnType = Any
FuncType = TypeVar('FuncType', bound=Callable[..., ReturnType])
def delayed(func: FuncType) -> Callable[..., Delayed[ReturnType]]:
    ...
