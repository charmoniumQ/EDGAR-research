import sys
from edgar_code.util.time_code import time_code
from edgar_code.retrieve import rfs_for


def main():
    for year in range(2013, 2019):
    # for year in range(1993, 2019):
        for qtr in range(1, 5):
            with time_code(f'get_all_rfs({year}, {qtr})'):
                size = (
                    rfs_for(year, qtr)

                    # this reduction helps it fit in memory
                    .map_values(len)
                    .values()
                    .sum()

                    .compute()
                )
                print(f'{year},{qtr},{size}')
                sys.stdout.flush()


if __name__ == '__main__' or True:
    main()
