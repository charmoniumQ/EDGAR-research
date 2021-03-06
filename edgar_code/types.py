from __future__ import annotations
from typing import TYPE_CHECKING, TypeVar, Union, Iterable, IO, Any
from collections import UserDict as _UserDict
from pathlib import PurePath
from typing_extensions import Protocol
from dask.delayed import Delayed as _Delayed
from dask.bag import Bag as _Bag
from distributed import Future as _Future


# TODO: assess if these are still necessary even with
# from __future__ import annotations

# https://stackoverflow.com/a/48554601/1078199
if TYPE_CHECKING:
    # we are running in mypy
    # which understands Bag[T]
    Bag = _Bag
    UserDict = _UserDict
    Future = _Future
    Delayed = _Delayed
else:
    # I need to make `Bag` subscriptable
    # subscripting FakeGenericMeta is a no-op
    # so `Bag[T] is Bag`
    class FakeBagMeta(type(_Bag)):
        def __getitem__(cls, item):
            return cls
    class Bag(_Bag, metaclass=FakeBagMeta):
        pass

    class FakeUserDictMeta(type(_UserDict)):
        def __getitem__(cls, item):
            return cls
    class UserDict(_UserDict, metaclass=FakeUserDictMeta):
        pass

    class FakeFutureMeta(type(_Future)):
        def __getitem__(cls, item):
            return cls
    class Future(_Future, metaclass=FakeFutureMeta):
        pass

    class FakeDelayedMeta(type(_Delayed)):
        def __getitem__(cls, item):
            return cls
    class Delayed(_Delayed, metaclass=FakeDelayedMeta):
        pass

ResultT = TypeVar('ResultT')
Result = Union[ResultT, Exception]

class PathLike(Protocol):
    # pylint: disable=no-self-use,unused-argument
    def __truediv__(self, other: Union[str, PurePath]) -> PathLike:
        ...
    def mkdir(self, mode: int = 0, parents: bool = False, exist_ok: bool = False) -> None:
        ...
    def exists(self) -> bool:
        ...
    def unlink(self) -> None:
        ...
    def iterdir(self) -> Iterable[PathLike]:
        ...
    # pylint: disable=too-many-arguments
    def open(self, mode: str = 'r') -> IO[Any]:
        ...
    @property
    def parent(self) -> PathLike:
        ...

class Serializer(Protocol):
    # pylint: disable=unused-argument,no-self-use
    def load(self, fil: IO[bytes]) -> Any:
        ...
    def dump(self, obj: Any, fil: IO[bytes]) -> None:
        ...
