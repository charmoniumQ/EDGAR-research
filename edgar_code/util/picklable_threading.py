import threading
from typing import Optional, Type, Dict, Any
from types import TracebackType


# No synchronization neeed take place between Python processes
# Dask will try to pickle the CachedFunction, so this needs to be picklable
class ThreadLocalData(threading.local):
    def __getstate__(self) -> Dict[str, Any]:
        return dict()

    def __setstate__(self, _state: Dict[str, Any]) -> None:
        pass


class RLock:
    def __init__(self) -> None:
        self.rlock = threading.RLock()

    def acquire(self, blocking: bool = True, timeout: int = 1) -> bool:
        return self.rlock.acquire(blocking, timeout)

    def release(self) -> None:
        return self.rlock.release()

    def __enter__(self) -> None:
        self.rlock.__enter__()

    def __exit__(
            self,
            exc_type: Optional[Type[Exception]],
            exc_value: Optional[Exception],
            traceback: Optional[TracebackType]
    ) -> Optional[bool]:
        return self.rlock.__exit__(exc_type, exc_value, traceback)

    def __getstate__(self) -> Dict[str, Any]:
        return dict()

    def __setstate__(self, dct: Dict[str, Any]) -> None:
        pass
