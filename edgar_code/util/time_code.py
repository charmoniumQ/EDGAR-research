import logging
import datetime
import contextlib
logging.basicConfig(level=logging.INFO)


@contextlib.contextmanager
def time_code(msg=None):
    dct = {}
    start = datetime.datetime.now()
    logging.info('started: %s', msg)
    try:
        yield dct
    finally:
        stop = datetime.datetime.now()
        time = stop - start
        dct['time'] = time
        if msg:
            logging.info('%ss: %s', time, msg)


def add_time(func):
    def wrapped_func(*args, **kwargs):
        with time_code() as dct:
            result = func(*args, **kwargs)
        if 'time' not in result:
            result['time'] = dict(total=datetime.timedelta(0))
        result['time']['total'] += dct['time']
        result['time'][func.__name__] = dct['time']
        return result
    return wrapped_func
