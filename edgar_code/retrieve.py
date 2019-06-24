import functools
from typing import Union, Callable, TypeVar, List
import dask.bag
import edgar_code.parse as parse
import edgar_code.cli.config as config
from edgar_code.types import Bag, Result
from edgar_code.parse import Index
from edgar_code.util.time_code import time_code


npartitions = 100
@config.cache_decor
@time_code.decor(print_start=False, print_time=False)
def get_indexes(form_type: str, year: int, qtr: int) -> Bag[Index]:
    return dask.bag.from_sequence(
        parse.download_indexes(form_type, year, qtr),
        npartitions=npartitions)


@config.cache_decor
@time_code.decor(print_start=False, print_time=False)
def get_raw_forms(form_type: str, year: int, qtr: int) -> Bag[Result[bytes]]:
    def get_raw_forms_f(index: Index) -> bytes:
        with time_code.ctx('download sgml', print_start=False, print_time=False):
            sgml = parse.download_retry(index.url)
        with time_code.ctx('parse sgml', print_start=False, print_time=False):
            fileinfos = parse.sgml2fileinfos(sgml)
            raw_form = parse.find_form(fileinfos, form_type)
        return raw_form

    return (
        get_indexes(form_type, year, qtr)
        .map(make_try_func(get_raw_forms_f))
    )


@config.cache_decor
@time_code.decor(print_start=False, print_time=False)
def get_paragraphs(form_type: str, year: int, qtr: int) -> Bag[Result[List[str]]]:
    @time_code.decor(print_start=False, print_time=False)
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


@config.cache_decor
@time_code.decor(print_start=False, print_time=False)
def get_rfs(year: int, qtr: int) -> Bag[Result[List[str]]]:
    def paragraphs2rf(paragraphs: List[str]) -> List[str]:
        return parse.paragraphs2rf(paragraphs, year < 2006)
    return (
        get_paragraphs('10-K', year, qtr)
        .map(make_try_func(paragraphs2rf))
    )


#### Helpers ####

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')
def make_try_func(func: Callable[[T], U]) -> Callable[[Result[T]], Result[U]]:
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
