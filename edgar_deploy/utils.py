class Struct(object):
    pass


def omit(dct, keys):
    '''omit keys from a dict'''
    return {key: val for key, val in dct.items() if key not in keys}


def flatten_iter(it, level=0):
    '''Lazily flattens an interator of iterators of ... of iterators
Yields (level, item) where item is a string or non-iterable type.'''
    for item in it:
        if isinstance(item, str) or not hasattr(item, '__next__'):
            yield level, item
        else:
            yield from flatten_iter(item, level+1)


def flatten1(list_):
    '''[[X]] -> [X]'''
    return [elem for list2 in list1 for elem in list2]


def empty_iter(itera):
    '''Test if an iterator is empty'''
    itera = iter(itera)
    try:
        itera.next()
    except Stopiteration:
        return True
    else:
        return False


import logging
import datetime
import contextlib
import collections
import threading
@contextlib.contextmanager
def time_code(name, print_start=True, print_time=True):
    '''Context that prints the wall-time taken to run the code inside.

    >>> with time_code('main stuff'):
    ...     pass # do stuff
    ...     with time_code('inner stuff'):
    ...         pass # do stuff
    ...
    main_stuff: starting
    main_stuff > inner_stuff: starting
    main_stuff > inner_stuff: 2.3s
    main_stuff: 3.3s


You can also access time_code.data.stats for a dict of qualified_names
to time-deltas describing the duration of the code.

It is like line- or function-profiling, but sometimes that is too
verbose, and you really only want start/end times for the hefty part
of your code. Perhaps this is useful to let the user know what you are
doing and why it takes long.

    '''

    if not hasattr(time_code.data, 'stack'):
        # this should be thread local so it can be used in a
        # multi-threaded environment (e.g. ThreadPoolExecutor)
        time_code.data.stack = []
        time_code.data.stats = collections.defaultdict(list)
        
    time_code.data.stack.append(name)
    qualified_name_str = ' > '.join(time_code.data.stack)
    qualified_name_tuple = tuple(time_code.data.stack)
    if print_start:
        logging.info(f'{qualified_name_str}: running')
    start = datetime.datetime.now()
    stack = time_code.data.stack
    try:
        yield
    finally:
        stop = datetime.datetime.now()
        duration = stop - start
        time_code.data.stack.pop()
        time_code.data.stats[qualified_name_tuple].append(duration)
        if print_time:
            seconds = duration.total_seconds()
            logging.info(f'{qualified_name_str}: {seconds:.1f}s')
time_code.data = threading.local()

import functools
def time_code_decor(**time_code_kwargs):
    '''Decorator for time_code

    >>> @time_code_decor(**kwargs_for_time_code)
    ... class Foo(object):
    ...     def foo(self):
    ...         pass # do stuff
    ... Foo().foo()
    ...
    Foo.foo: starting
    Foo.foo: 1.2s
'''
    def wrapper(func):
        @functools.wraps(func)
        def timed_func(*func_args, **func_kwargs):
            with time_code(func.__qualname__, **time_code_kwargs):
                return func(*func_args, **func_kwargs)
        return timed_func
    return wrapper


import time
def exp_backoff(initial, exp, max_):
    delay = initial
    while True:
        yield
        logging.info(f'delaying {delay:.1f}s')
        time.sleep(delay)
        delay = min(delay * exp, max_)


import datetime
unix_epoch = datetime.datetime(year=1970, month=1, day=1)
def timestamp_to_datetime(timestamp):
    return unix_epoch + datetime.timedelta(seconds=timestamp)


def datetime_to_datetime(datetime_obj):
    return (datetime_obj - unix_epoch).total_seconds()


import random
import string
def rand_name(n=10, lowercase=False, uppercase=False, digits=False, upperhexdigits=False, lowerhexdigits=False, custom_alphabet=''):
    alphabet = ''
    if lowercase:
        alphabet += string.ascii_lowercase
    if uppercase:
        alphabet += string.ascii_uppercase
    if digits:
        alphabet += string.digits
    if upperhexdigits:
        alphabet += '0123456789ABCDEF'
    if lowerhexdigits:
        alphabet += '0123456789abcdef'
    if custom_alphabet:
        alphabet += custom_alphabet
    alphabet = list(set(alphabet))
    return ''.join(random.choice(alphabet) for _ in range(n))
