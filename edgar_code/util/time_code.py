from typing import (
    cast, Generator, Any, Callable, TypeVar,
    Tuple, Dict, List, Optional,
)
import functools
import gc
import os
import math
import copy
import logging
import datetime
import threading
import contextlib
import collections
import psutil
try:
    from edgar_code.util.picklable_threading import ThreadLocalData, RLock
except ImportError:
    # pylint: disable=ungrouped-imports
    from threading import RLock, local as ThreadLocalData # type: ignore


logger = logging.getLogger(__name__)


class _TimeCodeData(ThreadLocalData):
    def __init__(self) -> None:
        super().__init__()
        if threading.current_thread() is threading.main_thread():
            self.stack = ['']
        else:
            self.stack = ['Thread ' + threading.current_thread().name]


FunctionType = TypeVar('FunctionType', bound=Callable[..., Any])
class _TimeCode:
    def __init__(self) -> None:
        self.data = _TimeCodeData()
        self.lock = RLock()
        self.stats: Dict[Tuple[str, ...], List[Tuple[float, int]]] = collections.defaultdict(list)

    def get_stack(self) -> List[str]:
        '''Returns the current call-stack in annotated code.'''
        return self.data.stack

    def get_stats(self) -> Dict[Tuple[str, ...], List[Tuple[float, int]]]:
        '''Gets the stats for a specific function.'''
        # need lock to get consistent view of stats
        with self.lock:
            # need deepcopy so returned object doesn't change
            return copy.deepcopy(dict(self.stats))

    @contextlib.contextmanager
    def ctx(
            self, name: str, print_start: bool = True, print_time: bool = True,
            run_gc: bool = False,
    ) -> Generator[None, None, None]:
        '''Context that prints the wall-time taken to run the code inside.

    >>> time_code = _TimeCode()
    >>> import time
    >>> with time_code.ctx('main stuff'):
    ...     time.sleep(0.5)
    ...     with time_code.ctx('inner stuff'):
    ...         time.sleep(0.3)
    ...
    main_stuff: starting
     > main_stuff > inner_stuff: starting
     > main_stuff > inner_stuff: 0.3s
    main_stuff: 0.8s

You can also access time_code.stats for a dict of qualified_names
to time-deltas describing the duration of the code.

It is like function-profiling, but:
- Less verbose
- Lets you select what gets profiled and reported
- Less performance overhead
- Reported to the user (to let them know what the code is doing right now)
- Programmably accessible starting and results
- Does not need source-code access, so it will work from .eggs

        '''

        self.data.stack.append(name)
        qualified_name_str = ' > '.join(self.data.stack)
        if print_start:
            logger.debug('%s: running', qualified_name_str)
        exc: Optional[Exception] = None
        process = psutil.Process(os.getpid())
        time_start = datetime.datetime.now()
        mem_start = process.memory_info().rss
        try:
            yield
        except Exception as exc2: # pylint: disable=broad-except
            exc = exc2
        finally:
            time_stop = datetime.datetime.now()
            duration = (time_stop - time_start).total_seconds()
            if run_gc:
                gc_start = datetime.datetime.now()
                gc.collect()
                gc_end = datetime.datetime.now()
                gc_duration = (gc_end - gc_start).total_seconds()
            else:
                gc_duration = 0
            mem_end = process.memory_info().rss
            mem_leaked = mem_end - mem_start
            with self.lock:
                self.stats[tuple(self.data.stack[1:])].append(
                    (duration, mem_leaked)
                )
            self.data.stack.pop()
            if print_time:
                mem_val, mem_unit, _ = mem2str(mem_leaked)
                logger.debug(
                    '%s: %.1fs %.1f%s (gc: %.1fs) %s',
                    qualified_name_str,
                    duration,
                    mem_val,
                    mem_unit,
                    gc_duration,
                    ' (err)' if exc is not None else ''
                )
        if exc:
            raise exc

    def make_timed_func(
            self, func: FunctionType,
            print_start: bool = True, print_time: bool = True,
            print_args: bool = False, run_gc: bool = False,
    ) -> FunctionType:
        @functools.wraps(func)
        def timed_func(*args: Any, **kwargs: Any) -> Any:
            if print_args:
                arg_str = ''.join([
                    '(',
                    ', '.join(f'{arg!r}' for arg in args),
                    ', '.join(f'{key}={val!r}' for key, val in kwargs.items()),
                    ')',
                ])
            else:
                arg_str = ''
            with self.ctx(func.__qualname__ + arg_str, print_start, print_time, run_gc):
                return func(*args, **kwargs)
        return cast(FunctionType, timed_func)

    def decor(
            self, print_start: bool = True, print_time: bool = True,
            print_args: bool = False, run_gc: bool = False,
    ) -> Callable[[FunctionType], FunctionType]:
        '''Decorator for time_code

    >>> time_code = _TimeCode()
    >>> import time
    >>> @utils.time_code.decor()
    ... class Foo(object):
    ...     def foo(self):
    ...         time.sleep(0.3)
    ...
    >>> Foo().foo()
    ...
    Foo.foo: starting
    Foo.foo: 0.3s

        '''

        def make_timed_func(func: FunctionType) -> FunctionType:
            return self.make_timed_func(func, print_start, print_time, print_args, run_gc)
        return make_timed_func

    def format_stats(self) -> str:
        stats = {
            key: (
                len(vals),
                mean([time for time, mem in vals]),
                std([time for time, mem in vals]),
                mean([mem for time, mem in vals]),
                std([mem for time, mem in vals]),
            )
            for key, vals in self.get_stats().items()
        }

        keys = sorted(stats.keys())
        key_field_length = max(len(' > '.join(key)) for key in keys) if keys else 0

        lines: List[str] = []

        for key in keys:
            key_str = ' > '.join(key)

            n_calls = stats[key][0]
            cumulative_time_m = stats[key][1]
            cumulative_time_s = stats[key][2]
            mem_m, mem_unit, mem_unit_size = mem2str(stats[key][3])
            mem_s = stats[key][4] / mem_unit_size
            percall_time_m = cumulative_time_m / n_calls
            percall_time_s = cumulative_time_s * n_calls

            parent = key[:-1]
            if parent in stats:
                parent_total_time_m = stats[parent][1]
                percent_parent = cumulative_time_m / parent_total_time_m * 100
            else:
                percent_parent = 100

            total = key[:2]
            total_time_m = stats[total][1]
            percent_total = cumulative_time_m / total_time_m * 100

            lines.append(' = '.join([
                f'{key_str:{key_field_length}s}',
                f'{percent_total:2.0f}% of total',
                f'{percent_parent:2.0f}% of parent',
                f'({cumulative_time_m:.2f} +/- {cumulative_time_s:.2f}) sec',
                f'{n_calls} ({percall_time_m:.2f} +/- {percall_time_s:.2f}) sec',
            ]) + f'  ({mem_m:.1f} +/- {mem_s:.1f}) {mem_unit}')

        return '\n'.join(lines)

    def print_stats(self) -> None:
        print(self.format_stats())

    def add_stats(
            self, other_stats: Dict[Tuple[str, ...], List[Tuple[float, int]]]
    ) -> None:
        with self.lock:
            for key, times in other_stats.items():
                self.stats[key].extend(times)

    def clear(self) -> None:
        with self.lock:
            self.stats.clear()


def mem2str(n_bytes: float, base2: bool = True, round_up: bool = False) -> Tuple[float, str, float]:
    rounder = cast(Callable[[float], float], round) if round_up else math.floor
    unit_map: List[str] = ['b', 'Kb', 'Mb', 'Gb', 'Tb']
    base = 1024 if base2 else 1000
    unit_int = min([
        len(unit_map) - 1,
        int(rounder(math.log(math.fabs(n_bytes), base)))
    ]) if n_bytes != 0 else 0
    unit_div = base**unit_int
    return n_bytes / unit_div, unit_map[unit_int], unit_div

def mean(lst: List[float]) -> float:
    return sum(lst) / len(lst)

def std(lst: List[float]) -> float:
    if len(lst) != 1:
        m = mean(lst)
        return math.sqrt(sum((x - m)**2 for x in lst) / (len(lst) - 1))
    else:
        return 0


time_code = _TimeCode()


__all__ = ['time_code']
