from typing import cast, List
from pathlib import Path
import tempfile
from edgar_code.cache import Cache, MemoryStore, FileStore, DirectoryStore


def test_cache() -> None:
    with tempfile.TemporaryDirectory() as work_dir_:
        work_dir = Path(work_dir_)
        calls: List[int] = []

        @Cache.decor(MemoryStore.create())
        def square1(x: int, **kwargs: int) -> int: # pylint: disable=invalid-name
            calls.append(x)
            return x**2 + kwargs.get('a', 0)

        @Cache.decor(FileStore.create(str(work_dir)))
        def square2(x: int, **kwargs: int) -> int: # pylint: disable=invalid-name
            calls.append(x)
            return x**2 + kwargs.get('a', 0)

        @Cache.decor(DirectoryStore.create(work_dir / 'cache'))
        def square3(x: int, **kwargs: int) -> int: # pylint: disable=invalid-name
            calls.append(x)
            return x**2 + kwargs.get('a', 0)

        for square in [square1, square2, square3]:
            calls.clear()

            assert square(7) == 49 # miss
            assert square(2) == 4 # miss

            # repeated values should not miss; they will not show up in
            # calls
            assert square(7) == 49
            assert square(2) == 4

            # clearing cache should make next miss
            square_ = cast(Cache, square)
            square_.clear()

            # clearing cache should remove the file
            if hasattr(square_, 'cache_path'):
                assert not getattr(square_, 'cache_path').exists()

            assert square(7) == 49 # miss

            # adding a kwarg should make a miss
            assert square(7) == 49 # hit
            assert square(7, a=2) == 51 # miss

            # test del explicitly
            # no good way to test it no normal functionality
            del square_.obj_store[square_.obj_store.args2key((7,), {})]
            assert square(7) == 49

            assert calls == [7, 2, 7, 7, 7]
