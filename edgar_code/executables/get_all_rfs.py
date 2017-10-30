import json
from edgar_code.util import new_directory, sanitize_fname, unused_fname
from edgar_code.retrieve import rfs_for
from dask.diagnostics import ProgressBar


def main(year, qtr, dir_):
    pbar = ProgressBar()
    pbar.register()
    for record, rf in rfs_for(year, qtr).compute():
        starting_fname = sanitize_fname(record.company_name)
        fname = unused_fname(dir_, starting_fname).with_suffix('.txt')
        print('{record.company_name} -> {fname.name}'.format(**locals()))
        with fname.open('w', encoding='utf-8') as f:
            if rf:
                record_ = dict(**record._asdict())
                record_['date_filed'] = str(record_['date_filed'])
                f.write(json.dumps(record_))
                f.write('\n')
                f.write(rf)


if __name__ == '__main__':
    year = 2008
    qtr = 2
    dir_ = new_directory()
    print('results in', dir_)
    main(year, qtr, dir_)
