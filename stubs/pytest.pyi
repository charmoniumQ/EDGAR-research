from typing import ContextManager, Type


def raises(exc: Type[Exception]) -> ContextManager[None]:
    ...
