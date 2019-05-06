import functools
from typing import Union
import dask.bag
from edgar_code.cloud import cache_path, BagStore
from edgar_code.util import Cache
from edgar_code.type_stubs.dask.bag import Bag as BagType
from edgar_code.retrieve.form import (
    index2form_text, form_text2main_text, main_text2form_items,
    form_items2form_item, download_indexes, Index
)


# TODO: allow the caller to set the cache path and S3 credentials, instead of
# setting it here. Suppose a different application wants to use this as a
# library. They might want to cache it somewhere else, or not at all. It makes
# more logical sense that the caller in edgar_code.executables sets the cache
# path and S3 credentials.



cache_decor = Cache.decor(BagStore.create(cache_path / 'bags'), miss_msg=True)
#cache_decor = lambda x: x


# TODO: function signature for higher order function
def make_try_func(func):
    @functools.wraps(func)
    def try_func(*args, **kwargs):
        if isinstance(args[0], Exception):
            return args[0]
        else:
            try:
                return func(*args, **kwargs)
            except Exception as exc: #pylint: disable=broad-except
                return exc
    return try_func


npartitions = 100
@cache_decor
def get_indexes(form_type: str, year: int, qtr: int) -> BagType[Index]:
    return dask.bag.from_sequence(
        download_indexes(form_type, year, qtr),
        npartitions=npartitions)


# TODO: Result[T] = Union[T, Exception]
@cache_decor
def get_main_texts(form_type: str, year: int, qtr: int) -> BagType[Union[str, Exception]]:
    return (
        get_indexes(form_type, year, qtr)
        .map(make_try_func(functools.partial(index2form_text)(form_type)))
        .map(make_try_func(functools.partial(form_text2main_text)(form_type)))
    )


@cache_decor
def get_rfs(year: int, qtr: int) -> BagType[Union[str, Exception]]:
    return (
        get_main_texts('10-K', year, qtr)
        .map(make_try_func(functools.partial(main_text2form_items)('10-K')))
        .map(make_try_func(functools.partial(form_items2form_item)('Item 1A')))
    )
