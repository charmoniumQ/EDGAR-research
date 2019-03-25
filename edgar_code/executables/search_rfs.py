import json
import re
import toolz
from edgar_code.util import new_directory, sanitize_unused_fname
from edgar_code.retrieve import good_rfs_for
from dask.diagnostics import ProgressBar


@toolz.curry
def replace_or_none(pattern, repl, string):
    new_string, n_subst = pattern.subn(repl, string)
    if n_subst:
        return new_string
    else:
        return None


def main(year, qtr, dir_, pattern):
    pattern = re.compile(pattern)

    # issue cluster-compute work
    rfs = (
        good_rfs_for(year, qtr)
        .map_values(replace_or_none(pattern, r'<< \g<0> >>'))
        .filter_values(bool)
    )

    # collect and aggregate locally
    pbar = ProgressBar()
    pbar.register()
    for record, rf in rfs.compute():
        fname = sanitize_unused_fname(dir_, record.company_name, 'txt')
        print(f'{record.company_name} -> {fname.name}')
        with fname.open('w', encoding='utf-8') as f:
            record_ = dict(**record._asdict())
            record_['date_filed'] = str(record_['date_filed'])
            f.write(json.dumps(record_))
            f.write('\n')
            f.write(rf)


if __name__ == '__main__':
    year = 2008
    qtr = 1
    dir_ = new_directory()
    pattern = 'climate.change'
    main(year, qtr, dir_, pattern)
