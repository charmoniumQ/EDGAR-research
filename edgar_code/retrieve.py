import functools
from typing import Union, Callable, Any
import dask.bag
from edgar_code.cloud import cache_path, BagStore
from edgar_code.util import Cache
from edgar_code.type_stubs.dask.bag import Bag as BagType
import edgar_code.parse as parse
Index = parse.Index


# TODO: allow the caller to set the cache path and S3 credentials, instead of
# setting it here. Suppose a different application wants to use this as a
# library. They might want to cache it somewhere else, or not at all. It makes
# more logical sense that the caller in edgar_code.executables sets the cache
# path and S3 credentials.


cache_decor = Cache.decor(BagStore.create(cache_path / 'bags'), miss_msg=True)
#cache_decor = lambda x: x


# TODO: function signature for higher order function
def make_try_func(func: Callable[..., Any]) -> Callable[..., Any]:
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
        parse.download_indexes(form_type, year, qtr),
        npartitions=npartitions)


# TODO: Result[T] = Union[T, Exception]
@cache_decor
def get_raw_forms(form_type: str, year: int, qtr: int) -> BagType[Union[str, Exception]]:
    def get_raw_forms_f(index: Index) -> str:
        sgml = parse.download_retry(index.url)
        fileinfos = parse.sgml2fileinfos(sgml)
        raw_form = parse.find_form(fileinfos, form_type)
        return raw_form

    return (
        get_indexes(form_type, year, qtr)
        .map(make_try_func(get_raw_forms_f))
    )


@cache_decor
def get_main_texts(form_type: str, year: int, qtr: int) -> BagType[Union[str, Exception]]:
    def get_main_texts_f(raw_form: str) -> str:
        if parse.is_html(raw_form):
            raw_text = parse.html2text(raw_form)
        else:
            raw_text = parse.clean_sgml_text(raw_form)
        clean_text = parse.clean_text(raw_text)
        if form_type == '10-K':
            # special case, 10-K forms contain table of contents
            return parse.remove_header(clean_text)
        else:
            return clean_text

    return (
        get_indexes(form_type, year, qtr)
        .map(make_try_func(get_main_texts_f))
    )


@cache_decor
def get_rfs(year: int, qtr: int) -> BagType[Union[str, Exception]]:
    def get_rfs_f(clean_text: str) -> str:
        form_items = parse.main_text2form_items(clean_text, '10-K')
        try:
            return form_items['Item 1A']
        except KeyError as exc:
            raise parse.ParseError(str(exc))

    return (
        get_main_texts('10-K', year, qtr)
        .map(make_try_func(get_rfs_f))
    )


__all__ = ['get_rfs']
