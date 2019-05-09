from typing import Dict, Tuple, List
import math
import time
import collections
import pytest
from edgar_code.util import time_code


stats: Dict[Tuple[str, ...], List[float]] = collections.defaultdict(list)

@time_code.decor()
def f1() -> None:
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
    raise ValueError()

@time_code.decor()
def f2() -> None:
    stats[('f1', 'f2')].append(0)

    # context manager works just as well as function decorator
    with time_code.ctx('f3'):
        time.sleep(0.03)
        stats[('f1', 'f2', 'f3')].append(0.03)

        # we can get the current stack
        assert time_code.get_stack() == ['f1', 'f2', 'f3']
    stats[('f1', 'f2')][-1] += stats[('f1', 'f2', 'f3')][-1]

    time.sleep(0.07)
    stats[('f1', 'f2')][-1] += 0.07


def test_time_code() -> None:
    with pytest.raises(ValueError):
        # if exception occurs, it is propogated up
        f1()

    global stats
    stats = dict(stats)
    for key in time_code.get_stats().keys() | stats.keys():
        for expected_val, actual_val in zip(stats[key], time_code.get_stats()[key]):
            assert math.fabs(expected_val - actual_val) < 0.01, (stats, time_code.get_stats())
