from typing import ContextManager


def raises(exc: Exception) -> ContextManager[None]:
    ...
