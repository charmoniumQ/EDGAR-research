import json
from edgar_code.util import new_directory, sanitize_fname, unused_fname
from edgar_code.retrieve import rfs_for
from edgar_code.tokenize import text2section_word_stems, combine_counts
from dask.diagnostics import ProgressBar


from ..cloud import get_s3path
from ..util.cache import Cache, IndexInFile, CustomStore


index_cache = get_s3path('cache', 'cache')
object_cache = get_s3path('cache', 'cache')


@Cache(IndexInFile(index_cache), CustomStore(object_cache, dir_=object_cache), 'hit {name} with {key}', 'miss {name} with {key}')
def section_word_stems_for(year, qtr):
    # this is the cluster-compting part
    return (
        rfs_for(year, qtr)
        .filter_values(bool)
        .filter_values(lambda x: len(x) > 1000)
        .map_values(text2section_word_stems)
    )


def main(year, qtr, dir_):
    pbar = ProgressBar()
    pbar.register()
    data = section_word_stems_for(year, qtr).map_values(combine_counts)
    for record, (wc, sc) in data.compute():
        # this is collecting and storing the results locally

        starting_fname = sanitize_fname(record.company_name)
        fname = unused_fname(dir_, starting_fname).with_suffix('.txt')
        print(f'{record.company_name} -> {fname.name}')
        with fname.open('w', encoding='utf-8') as f:
            record_ = dict(**record._asdict())
            record_['date_filed'] = str(record_['date_filed'])
            f.write(json.dumps(record_))
            f.write('\n')
            for word, freq in sc.most_common(100):
                f.write(f'{freq},{word}\n')
            f.write('\n')
            for word, freq in wc.most_common(100):
                f.write(f'{freq},{word}\n')


if __name__ == '__main__':
    year = 2008
    qtr = 2
    dir_ = new_directory()
    print('results in', dir_)
    main(year, qtr, dir_)

