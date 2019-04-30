import re
from edgar_code.retrieve import good_rfs_for
import dask.bag
from edgar_code.cloud import results_path, KVBag

def paragraph_containing(pattern):
    return re.compile(f'\n(?P<output>[^\n]*({pattern})[^\n]*)\n', re.IGNORECASE)

def main():
    years = range(2006, 2019)

    terms = [
        'tsunami',
        'deepwater.horizon',
        'climate.change|global.warming',
        'collaps|housing.market|recession|subprim|collateral',
        'ebola|influenza|zika',
        'tarrif',
        'obama',
        'hack|cyber',
        'crimea',
        'brexit',
    ]

    dire = (results_path / f'search')
    dire.rmtree()

    patterns = list(map(paragraph_containing, terms))
    matches_arr = [[] for pattern in patterns]
    for key, pattern_matches in (
        KVBag.concat([
            good_rfs_for(year, qtr)
            for year in years for qtr in range(1, 5)
        ])
        .map_values(lambda rf: [[match.group('output') for match in pattern.finditer(rf)] for pattern in patterns])
        .filter_values(lambda lst: any(lst)) # helps cut some of the data transfer
        .compute()
    ):
        for pattern_no, matches in enumerate(pattern_matches):
            if matches:
                matches_arr[pattern_no].append((key, matches))

    for pattern_no, key_matches in enumerate(matches_arr):
        with (dire / f'{terms[pattern_no]}.txt').open('w') as f:
            for key, matches in key_matches:
                f.write(f'{key.company_name} (CIK {key.CIK}) Y{key.year}-Q{key.qtr}\n')
                for match in matches:
                    f.write(match.replace('\n', '\\n'))
                    f.write('\n')
                f.write('\n')
