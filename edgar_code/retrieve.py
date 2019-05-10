import functools
from typing import Union, Callable, TypeVar, List, TYPE_CHECKING
import dask.bag
from dask.bag import Bag as _Bag
import edgar_code.parse as parse
import edgar_code.config as config
from edgar_code.cache import Cache
from edgar_code.bag_store import BagStore


#### Types ####

# https://stackoverflow.com/a/48554601/1078199
if TYPE_CHECKING:
    # we are running in mypy
    # which understands Bag[T]
    Bag = _Bag
else:
    class FakeGenericMeta(type(_Bag)):
        def __getitem__(cls, item):
            return cls

    # I need to make `Bag` subscriptable
    # subscripting FakeGenericMeta is a no-op
    # so `Bag[T] is Bag`
    class Bag(_Bag, metaclass=FakeGenericMeta):
        pass

T = TypeVar('T')
U = TypeVar('U')
Result = Union[T, Exception]
Index = parse.Index


#### Config ####

# This is not a library. This is an application. Hence the S3
# credentials and cache path are set by the config module.  It is
# somewhat configurable through edgar_code.config, and modifying the
# Cache instances directly.

if config.cache:
    cache_decor = Cache.decor(
        BagStore.create(config.cache_path / 'bags'), miss_msg=True
    )
else:
    cache_decor = lambda x: x


#### Main ####

npartitions = 100
@cache_decor
def get_indexes(form_type: str, year: int, qtr: int) -> Bag[Index]:
    return dask.bag.from_sequence(
        parse.download_indexes(form_type, year, qtr),
        npartitions=npartitions)


@cache_decor
def get_raw_forms(form_type: str, year: int, qtr: int) -> Bag[Result[bytes]]:
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
def get_paragraphs(form_type: str, year: int, qtr: int) -> Bag[Result[List[str]]]:
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
def get_rfs(year: int, qtr: int) -> Bag[Result[List[str]]]:
    return (
        get_paragraphs('10-K', year, qtr)
        .map(make_try_func(lambda para: parse.paragraphs2rf(para, year >= 2006)))
    )


#### Helpers ####

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
