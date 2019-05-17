from typing import Dict, Tuple, List
from concurrent.futures import ThreadPoolExecutor
import copy
import time
import collections
import math
import pytest
from edgar_code.util import time_code


class UniqueExceptionType(Exception):
    pass

stats: Dict[Tuple[str, ...], List[float]] = collections.defaultdict(list)

# run_gc = True just for better test coverage
# on f1 it won't affect the timing stats collected
@time_code.decor(run_gc=True)
def f1() -> None: # pylint: disable=invalid-name
    stats[('f1',)].append(0)

    # includes the time of callees
    f2()
    stats[('f1',)][-1] += stats[('f1', 'f2')][-1]

    # works if you call the same func twice
    f2()
    stats[('f1',)][-1] += stats[('f1', 'f2')][-1]

    time.sleep(0.1)
    stats[('f1',)][-1] += 0.1

    # times code even if exception occurs
    raise UniqueExceptionType()

@time_code.decor(run_gc=False)
def f2() -> None: # pylint: disable=invalid-name
    stats[('f1', 'f2')].append(0)

    # context manager works just as well as function decorator
    with time_code.ctx('f3'):
        time.sleep(0.03)
        stats[('f1', 'f2', 'f3')].append(0.04)

        # we can get the current stack
        assert time_code.get_stack()[1:] == ['f1', 'f2', 'f3']
    stats[('f1', 'f2')][-1] += stats[('f1', 'f2', 'f3')][-1]

    time.sleep(0.07)
    stats[('f1', 'f2')][-1] += 0.08


def test_time_code() -> None:
    # if exception occurs, it is propogated up,
    # but time_code still takes measurements
    with pytest.raises(UniqueExceptionType):
        f1()

    # unfortunately stats will not be reliable because race conditions.
    # I'll take a snapshot of it and just use that
    global stats # pylint: disable=global-statement
    print('hewlihf', stats)
    stats = copy.deepcopy(stats)

    # time_code works in multithreaded mode
    # it maintains separate stacks per thread
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(f1) for _ in range(3)]
    for future in futures:
        # remember time_code propagates exceptions
        with pytest.raises(UniqueExceptionType):
            future.result()

    expected_stats = dict(stats)
    actual_stats = time_code.get_stats()
    for key in actual_stats.keys() | expected_stats.keys():
        expected_val = expected_stats[key][0]
        for actual_time_val, _ in actual_stats[key]:
            assert math.fabs(expected_val - actual_time_val) < 0.04, \
                (key, expected_val, actual_stats[key])
            # memory is really inconsistent
            # assert actual_mem_val == 0
    time_code.print_stats()
