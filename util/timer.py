import datetime
import contextlib

@contextlib.contextmanager
def timer(print_=False):
    dct = {}
    start = datetime.datetime.now()
    try:
        yield dct
    finally:
        stop = datetime.datetime.now()
        dct['time'] = stop - start
        if print_:
            print('Took {time!s}'.format(**locals()))


def add_time(func):
    def wrapped_func(*args, **kwargs):
        with timer() as dct:
            result = func(*args, **kwargs)
        if 'time' not in result:
            result['time'] = dict(total=datetime.timedelta(0))
        result['time']['total'] += dct['time']
        result['time'][func.__name__] = dct['time']
        return result
    return wrapped_func
