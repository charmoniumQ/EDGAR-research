import functools
from typing import Union, Callable, Any, Generic, TypeVar, List, cast
import dask.bag
from edgar_code.cloud import cache_path, BagStore
from edgar_code.cache import Cache
import edgar_code.parse as parse


Index = parse.Index
Bag = dask.bag.Bag


# TODO: allow the caller to set the cache path and S3 credentials, instead of
# setting it here. Suppose a different application wants to use this as a
# library. They might want to cache it somewhere else, or not at all. It makes
# more logical sense that the caller in edgar_code.executables sets the cache
# path and S3 credentials.


cache_decor = Cache.decor(BagStore.create(cache_path / 'bags'), miss_msg=True)
#cache_decor = lambda x: x


T = TypeVar('T')
U = TypeVar('U')
def make_try_func(func: Callable[[T], U]) -> Callable[[Union[T, Exception]], Union[U, Exception]]:
    @functools.wraps(func)
    def try_func(arg: Union[T, Exception]) -> Union[U, Exception]:
        if isinstance(arg, Exception):
            return arg
        else:
            try:
                return func(arg)
            except Exception as exc: #pylint: disable=broad-except
                return exc
    return try_func


npartitions = 100
@cache_decor
def get_indexes(form_type: str, year: int, qtr: int) -> Bag[Index]:
    return dask.bag.from_sequence(
        parse.download_indexes(form_type, year, qtr),
        npartitions=npartitions)


# TODO: Result = lambda T: Union[T, Exception]
@cache_decor
def get_raw_forms(form_type: str, year: int, qtr: int) -> Bag[Union[bytes, Exception]]:
    def get_raw_forms_f(index: Index) -> bytes:
        sgml = parse.download_retry(index.url)
        fileinfos = parse.sgml2fileinfos(sgml)
        raw_form = parse.find_form(fileinfos, form_type)
        return raw_form

    return (
        get_indexes(form_type, year, qtr)
        .map(make_try_func(get_raw_forms_f))
    )


@cache_decor
def get_paragraphs(form_type: str, year: int, qtr: int) -> Bag[Union[List[str], Exception]]:
    def get_main_texts_f(raw_form: bytes) -> List[str]:
        if parse.is_html(raw_form):
            raw_text = parse.html2paragraphs(raw_form)
        else:
            raw_text = parse.text2paragraphs(raw_form)
        clean_paragraphs = parse.clean_paragraphs(raw_text)
        if form_type == '10-K':
            # special case, 10-K forms contain table of contents
            return parse.remove_header(clean_paragraphs)
        else:
            return clean_paragraphs

    return (
        get_raw_forms(form_type, year, qtr)
        .map(make_try_func(get_main_texts_f))
    )


@cache_decor
def get_rf_paragraphs(year: int, qtr: int) -> Bag[Union[List[str], Exception]]:
    return (
        get_paragraphs('10-K', year, qtr)
        .map(make_try_func(lambda para: parse.paragraphs2rf(para, year >= 2006)))
    )


__all__ = ['get_rfs']
