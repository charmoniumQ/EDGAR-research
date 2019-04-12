from edgar_code.retrieve import rfs_for
from edgar_code.tokenize2 import text2paragraphs
from edgar_code.cloud import cache_path
from edgar_code.util.cache import Cache, DirectoryStore


@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True)
def section_word_stems_for(year, qtr):
    # this is the cluster-compting part
    def is_good_rf(rf):
        return len(x) > 1000
    def index_to_keys(oldkey):
        index, n = oldkey
        return (index.year, index.CIK, n)
    return (
        rfs_for(year, qtr)
        .filter_values(is_good_rf)
        .map_values(text2paragraphs)
        .flatmap_values()
        .map_keys(index_to_keys)
        .map_values(text2ws_counts)
    )


def main():
    for year in range(2006, 2019):
        for qtr in range(1, 5):
            section_word_stems_for(year, qtr)


if __name__ == '__main__':
    main()

