from typing import cast
import pytest
from edgar_code.cache import Cache
import edgar_code.retrieve as retrieve


@pytest.mark.slow
def test_retrieve() -> None:
    # disable the cache, because I don't want to persist these results
    # in the cloud
    for cached_func in [retrieve.get_rfs, retrieve.get_paragraphs,
                        retrieve.get_raw_forms, retrieve.get_indexes]:
        cast(Cache, cached_func).disabled = True
    list(retrieve.get_rfs(1995, 1).take(5))
