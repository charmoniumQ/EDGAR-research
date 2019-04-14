import collections
import sys
from edgar_code.util.time_code import time_code
from edgar_code.retrieve.combined import main_texts_for
from edgar_code.cloud import results_path


def is_good_mt(main_text):
    if isinstance(main_text, str):
        if len(main_text) > 1000:
            return main_text
        else:
            return 1
    else:
        return main_text


def is_good_rf(rf):
    if isinstance(rf, str):
        if len(rf) > 100:
            return rf
        else:
            return 2
    else:
        return rf


def main():
    for year in range(1993, 2002):
        for qtr in range(1, 5):
            with time_code(f'get_all_main_texts_for {year}, {qtr}'):
                results = dict(
                    main_texts_for('10-K', year, qtr)
                    .map_values(is_good_mt)
                    .map_values(is_good_rf)
                )
                counter = collections.Counter(results.values())
                print(f'{year} {qtr} {counter[1]} bad main_text {counter[2]} bad rfs')
                with (results_path / 'parse_errors' / f'{year}_{qtr}.csv').open('w') as f:
                    csvw = csv.writer(f)
                    for index, result in results.items():
                        csvw.writerow([
                            str(index.year),
                            str(index.qtr),
                            index.company_name,
                            index.url,
                            str(result) if isinstance(result, int) else '0',
                            str(len(result)) if isinstance(result, str) else '0',
                        ])


if __name__ == '__main__' or True:
    main()
