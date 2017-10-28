import toolz
from ..util import cache
from .index import download_indexes
from .form import index_to_form_text, form_text_to_main_text, main_text_to_form_items
from .rf import form_items_to_rf


#@cache
def indexs_for(form_type, year, qtr):
    return download_indexes(form_type, year, qtr)


#@cache
def form_texts_for(form_type, year, qtr):
    return indexes_for(form_type, year, qtr) \
        .map_items(index_to_form_text(form_type))


#@cache
def main_texts_for(form_type, year, qtr):
    return form_text_for(form_type, year, qtr) \
        .map_items(form_text_to_main_text(form_type))


#@cache
def form_itemss_for(form_type, year, qtr):
    return main_text_for(form_type, year, qtr) \
        .map_items(main_text_to_form_items(form_type))


#@cache
def rfs_for(form_type, year, qtr):
    return main_text_for(form_type, year, qtr) \
        .map_items(form_items_to_rf(form_type))

# no cache because not much work to compute
def good_rfs_for(form_type, year, qtr):
    return rfs_for(form_type, year, qtr).filter_values(bool)


def index_to_rf(index):
    return toolz.pipe(
        index,
        index_to_form_text('10-K'),
        form_text_to_main_text('10-K'),
        main_text_to_form_items('10-K'),
        form_items_to_rf,
    )
