import json
from edgar_code.util import new_directory, sanitize_fname, unused_fname
from edgar_code.retrieve import rfs_for
from edgar_code.tokenize import stem_text, count
from dask.diagnostics import ProgressBar


def main(year, qtr, dir_):
    pbar = ProgressBar()
    pbar.register()
    data = (
        rfs_for(year, qtr)
        .filter_values(bool)
        .filter_values(lambda x: len(x) > 100)
        .map_values(stem_text)
        .map_values(count)
    )
    for record, (counter, stems_froom) in data.compute():
        starting_fname = sanitize_fname(record.company_name)
        fname = unused_fname(dir_, starting_fname).with_suffix('.txt')
        print('{record.company_name} -> {fname.name}'.format(**locals()))
        with fname.open('w', encoding='utf-8') as f:
            record_ = dict(**record._asdict())
            record_['date_filed'] = str(record_['date_filed'])
            f.write(json.dumps(record_))
            f.write('\n')
            for word, freq in counter.most_common(100):
                f.write('{freq},{word}\n'.format(**locals()))


if __name__ == '__main__':
    year = 2008
    qtr = 2
    dir_ = new_directory()
    print('results in', dir_)
    main(year, qtr, dir_)

