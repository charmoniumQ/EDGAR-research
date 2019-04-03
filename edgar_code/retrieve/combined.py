import toolz
from ..cloud import cache_path, BagStore, KVBag
from ..util.cache import Cache, IndexInFile, FileStore
from .index import download_indexes
from .form import index_to_form_text, form_text_to_main_text, main_text_to_form_items
from .rf import form_items_to_rf
from .directors import form_items_to_directors


# TODO: allow the caller to set the cache path and S3 credentials, instead of
# setting it here. Suppose a different application wants to use this as a
# library. They might want to cache it somewhere else, or not at all. It makes
# more logical sense that the caller in edgar_code.executables sets the cache
# path and S3 credentials.


cache_decor = Cache.decor(IndexInFile.create(cache_path / 'index'), [BagStore.create(cache_path / 'bag')])
#cache_decor = lambda x: x


def indexes_for(form_type, year, qtr):
    return KVBag.from_bag(
        download_indexes(form_type, year, qtr)
        .map(lambda index_row: (index_row, index_row))
    )


def form_texts_for(form_type, year, qtr):
    return indexes_for(form_type, year, qtr) \
        .map_values(index_to_form_text(form_type))


# TODO: remove this extraneous cache
def main_texts_for(form_type, year, qtr):
    return form_texts_for(form_type, year, qtr) \
        .map_values(form_text_to_main_text(form_type))


def form_itemss_for(form_type, year, qtr):
    '''
    :return: bag of dictionaries where each dict has a form-heading as its key, and the text of that form-heading as its value.
    For example:
    {'Item 1A': 'This is the section 1a text here.', 'Item 2': 'This is the section 2 text'}
    '''
    return main_texts_for(form_type, year, qtr) \
        .map_values(main_text_to_form_items(form_type))


@cache_decor
def rfs_for(year, qtr):
    return form_itemss_for('10-K', year, qtr) \
        .map_values(form_items_to_rf)


def directors_for(year, qtr):
    """
    Departure of Directors (item 5.02, form 8k)
    :return:
    """
    return form_itemss_for('8-k', year, qtr) \
        .map_values(form_items_to_directors)

# no cache because not much work to compute
def good_rfs_for(form_type, year, qtr):
    return rfs_for(form_type, year, qtr).filter_values(bool)


def index_to_rf(index):
    '''Not cached. Useful for getting a few indexes to rfs, rather than the
whole quarter. If you want the whole quarter, use rfs_for because it's
cached.'''
    return toolz.pipe(
        index,
        index_to_form_text('10-K'),
        form_text_to_main_text('10-K'),
        main_text_to_form_items('10-K'),
        form_items_to_rf,
    )


def index_to_directors(index):
    return toolz.pipe(
        index,
        index_to_form_text('8-K'),
        form_text_to_main_text('8-K'),
        main_text_to_form_items('8-K'),
        form_items_to_directors
    )
