from typing import cast
import tempfile
from edgar_code.cache import Cache, MemoryStore, FileStore, DirectoryStore


def test_cache() -> None:
    with tempfile.TemporaryDirectory() as cache_dir:
        calls = []

        @Cache.decor(MemoryStore.create())
        def square1(x: int) -> int: # pylint: disable=invalid-name
            calls.append(x)
            return x**2

        @Cache.decor(FileStore.create(str(cache_dir)))
        def square2(x: int) -> int: # pylint: disable=invalid-name
            calls.append(x)
            return x**2

        @Cache.decor(DirectoryStore.create(cache_dir))
        def square3(x: int) -> int: # pylint: disable=invalid-name
            calls.append(x)
            return x**2

        for square in [square1, square2, square3]:
            calls.clear()
            square(7) # miss
            square(2) # miss
            square(7)
            square(2)
            cast(Cache, square).clear()
            square(7) # miss

        assert calls == [7, 2, 7]
