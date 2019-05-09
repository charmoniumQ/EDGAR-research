import functools
from typing import cast, Generator, Any, Callable, TypeVar, Tuple, Dict, List, Optional
import logging
import datetime
import contextlib
import collections


FuncType = Callable[..., Any]
F = TypeVar('F', bound=FuncType)


class _TimeCode:
    def __init__(self):
        self.stack: List[str] = []
        self.stats: Dict[Tuple[str, ...], List[float]] = collections.defaultdict(list)

    def get_stack(self) -> List[str]:
        '''Returns the current call-stack in annotated code.'''
        return self.stack

    def get_stats(self) -> Dict[Tuple[str, ...], List[float]]:
        '''Gets the stats for a specific function.'''
        return dict(self.stats)

    @contextlib.contextmanager
    def ctx(self, name: str, print_start=True, print_time=True) -> Generator[None, None, None]:
        '''Context that prints the wall-time taken to run the code inside.

    >>> time_code = _TimeCode()
    >>> import time
    >>> with time_code.ctx('main stuff'):
    ...     time.sleep(0.5)
    ...     with time_code.ctx('inner stuff'):
    ...         time.sleep(0.3)
    ...
    main_stuff: starting
    main_stuff > inner_stuff: starting
    main_stuff > inner_stuff: 0.3s
    main_stuff: 0.8s

You can also access time_code.data.stats for a dict of qualified_names
to time-deltas describing the duration of the code.

It is like line- or function-profiling, but sometimes that is too
verbose, and you really only want start/end times for the hefty part
of your code. Perhaps this is useful to let the user know what you are
doing and why it takes long. It also does not affect performance as
much as general profiling.

        '''
        self.stack.append(name)
        qualified_name_str = ' > '.join(self.stack)
        if print_start:
            logging.info('%s: running', qualified_name_str)
        exc: Optional[Exception] = None
        start = datetime.datetime.now()
        try:
            yield
        except Exception as exc2: # pylint: disable=broad-except
            exc = exc2
        finally:
            stop = datetime.datetime.now()
            duration = (stop - start).total_seconds()
            self.stats[tuple(self.stack)].append(duration)
            self.stack.pop()
            if print_time:
                logging.info(
                    '%s: %.1fs %s',
                    qualified_name_str,
                    duration,
                    ' (err)' if exc is not None else ''
                )
        if exc:
            raise exc

    def decor(self, print_start=True, print_time=True) -> Callable[[F], F]:
        '''Decorator for time_code

    >>> time_code = _TimeCode()
    >>> import time
    >>> @time_code.decor()
    ... class Foo(object):
    ...     def foo(self):
    ...         time.sleep(0.3)
    ...
    >>> Foo().foo()
    ...
    Foo.foo: starting
    Foo.foo: 0.3s

        '''

        def make_timed_func(func: F) -> F:
            @functools.wraps(func)
            def timed_func(*func_args, **func_kwargs):
                with self.ctx(func.__qualname__, print_start, print_time):
                    return cast(F, func(*func_args, **func_kwargs))
            return cast(F, timed_func)
        return make_timed_func


time_code = _TimeCode()
