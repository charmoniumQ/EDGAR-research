import toolz
from ..cloud import get_s3path
from ..util.cache import Cache, IndexInFile, CustomStore
from .index import download_indexes
from .form import index_to_form_text, form_text_to_main_text, main_text_to_form_items
from .rf import form_items_to_rf


# TODO: allow the caller to set the cache path and S3 credentials, instead of
# setting it here. Suppose a different application wants to use this as a
# library. They might want to cache it somewhere else, or not at all. It makes
# more logical sense that the caller in edgar_code.executables sets the cache
# path and S3 credentials.

index_cache = get_s3path('cache', 'cache')
object_cache = get_s3path('cache', 'cache')


@Cache(IndexInFile(index_cache), CustomStore(object_cache, None, dir_=object_cache), 'hit {name} with {key}', 'miss {name} with {key}')
def indexes_for(form_type, year, qtr):
    return download_indexes(form_type, year, qtr)


@Cache(IndexInFile(index_cache), CustomStore(object_cache, None, dir_=object_cache), 'hit {name} with {key}', 'miss {name} with {key}')
def form_texts_for(form_type, year, qtr):
    return indexes_for(form_type, year, qtr) \
        .map_values(index_to_form_text(form_type))


# TODO: remove this extraneous cache
@Cache(IndexInFile(index_cache), CustomStore(object_cache, None, dir_=object_cache), 'hit {name} with {key}', 'miss {name} with {key}')
def main_texts_for(form_type, year, qtr):
    return form_texts_for(form_type, year, qtr) \
        .map_values(form_text_to_main_text(form_type))


@Cache(IndexInFile(index_cache), CustomStore(object_cache, None, dir_=object_cache), 'hit {name} with {key}', 'miss {name} with {key}')
def form_itemss_for(form_type, year, qtr):
    return main_texts_for(form_type, year, qtr) \
        .map_values(main_text_to_form_items(form_type))


@Cache(IndexInFile(index_cache), CustomStore(object_cache, None, dir_=object_cache), 'hit {name} with {key}', 'miss {name} with {key}')
def rfs_for(year, qtr):
    return form_itemss_for('10-K', year, qtr) \
        .map_values(form_items_to_rf)

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