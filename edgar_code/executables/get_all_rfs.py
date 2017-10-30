import json
from edgar_code.util import new_directory, sanitize_fname, unused_fname
from edgar_code.retrieve import rfs_for
from dask.diagnostics import ProgressBar


def main(year, qtr, dir_):
    pbar = ProgressBar()
    pbar.register()
    for record, rf in rfs_for(year, qtr).compute():
        print(record.company_name)
        starting_fname = unusanitize_fname(record.company_name)
        fname = unused_fname(dir_, starting_fname)
        with fname.open('w', encoding='utf-8') as f:
            record_ = dict(**record._asdict())
            record_['date_filed'] = str(record_['date_filed'])
            f.write(json.dumps(dict(**record._asdict())))
            f.write('\n')
            f.write(rf)


if __name__ == '__main__':
    year = 2008
    qtr = 2
    dir_ = new_directory()
    print('results in', dir_)
    main(year, qtr, dir_)
