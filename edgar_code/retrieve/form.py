import itertools
import functools
import datetime
from typing import Iterable, Dict
from edgar_code.util import download_retry
from edgar_code.retrieve import helpers


Index = helpers.Index


def download_indexes(form_type: str, year: int, qtr: int) -> Iterable[Index]:
    lines = iter(helpers.download_index_lines(year, qtr))
    col_names = helpers.parse_header(lines)
    indexes = helpers.parse_body(year, qtr, lines, col_names)
    for index in helpers.filter_form_type(indexes, form_type):
        assert index.form_type == form_type
        assert len(index.company_name) > 4
        assert index.CIK > 1000
        assert (
            datetime.date(year, 1, 1) + datetime.timedelta(days=(qtr-1) * 3 * 31)
            < index.date_filed <
            datetime.date(year, 1, 1) + datetime.timedelta(days=(qtr-0) * 3 * 31)
        )
        assert str(index.CIK) in index.url
        assert index.year == year
        assert index.qtr == qtr
        yield index


@functools.lru_cache()
def download_indexes_cached(form_type: str, year: int, qtr: int) -> Iterable[Index]:
    return list(itertools.islice(download_indexes(form_type, year, qtr), 0, 10))


def test_download_indexes() -> None:
    form_type = '10-K'
    for year in [1995, 2018]:
        download_indexes_cached(form_type, year, 1)


def index2form_text(form_type: str, index: Index) -> str:
    sgml = download_retry(index.url)
    fileinfos = helpers.sgml2fileinfos(sgml)
    return helpers.find_form(fileinfos, form_type)


def test_index2form_text() -> None:
    form_type = '10-K'
    for year in [1995, 2018]:
        for index in download_indexes_cached(form_type, year, 1):
            # test that it doesn't have an _unanticipated_ failure
            # I'll test that the parser works well later on
            # hence, suppress_errors
            try:
                form_text = index2form_text(form_type, index)
            except helpers.ParseError:
                pass
            else:
                assert len(form_text) > 1e4


def form_text2main_text(form_type: str, form_raw: str) -> str:
    if helpers.is_html(form_raw):
        html = form_raw
        clean_html = helpers.clean_html(html)
        raw_text = helpers.html2text(clean_html)
    else:
        raw_text = form_raw
    clean_text = helpers.clean_text(raw_text)
    if form_type == '10-K':
        # special case, 10-K forms contain table of contents
        return helpers.remove_header(clean_text)
    else:
        return clean_text


def test_form_text2main_text() -> None:
    form_type = '10-K'
    for year in [1995, 2018]:
        for index in download_indexes_cached(form_type, year, 1):
            try:
                form_text = index2form_text(form_type, index)
                main_text = form_text2main_text(form_type, form_text)
            except helpers.ParseError:
                pass
            else:
                # assert 'table of contents' not in main_text.lower()
                assert len(main_text) > 1e4


def main_text2form_items(form_type: str, main_text: str) -> Dict[str, str]:
    return helpers.main_text2form_items(main_text, form_type)


def test_main_text2form_items() -> None:
    form_type = '10-K'
    for year in [1995, 2018]:
        for index in download_indexes_cached(form_type, year, 1):
            try:
                form_text = index2form_text(form_type, index)
                main_text = form_text2main_text(form_type, form_text)
                main_text2form_items(form_type, main_text)
            except helpers.ParseError:
                pass


def form_items2form_item(form_item: str, form_items: Dict[str, str]) -> str:
    try:
        return form_items[form_item]
    except KeyError as exc:
        raise helpers.ParseError(str(exc))


def test_form_items2form_item() -> None:
    form_type = '10-K'
    for year in [1995, 2018]:
        for index in download_indexes_cached(form_type, year, 1):
            try:
                form_text = index2form_text(form_type, index)
                main_text = form_text2main_text(form_type, form_text)
                form_items = main_text2form_items(form_type, main_text)
                form_items2form_item('Item 1A', form_items)
            except helpers.ParseError:
                pass
