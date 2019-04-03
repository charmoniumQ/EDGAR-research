import json
from edgar_code.util import new_directory, sanitize_unused_fname
from edgar_code.util.time_code import time_code
from edgar_code.retrieve import rfs_for
from dask.diagnostics import ProgressBar


def main(year, qtr, dir_):
    # issue cluster-compute command
    rfs = rfs_for(year, qtr)
    
    # collect results locally
    pbar = ProgressBar()
    pbar.register()
    for record, rf in rfs.compute():
        fname = sanitize_unused_fname(dir_, record.company_name, 'txt')
        print(f'{record.company_name} -> {fname.name}')
        # with fname.open('w', encoding='utf-8') as f:
        #     if rf:
        #         record_ = dict(**record._asdict())
        #         record_['date_filed'] = str(record_['date_filed'])
        #         f.write(json.dumps(record_))
        #         f.write('\n')
        #         f.write(rf)


if __name__ == '__main__' or True:
    year = 2008
    qtr = 1
    dir_ = new_directory()
    with time_code('get_all_rfs'):
        main(year, qtr, dir_)
