from typing import ContextManager, Type, TypeVar, Callable, Any


def raises(exc: Type[Exception]) -> ContextManager[None]:
    ...

F = TypeVar('F', bound=Callable[..., Any])
class Mark:
    def slow(self, func: F) -> F:
        ...

mark: Mark = ...
