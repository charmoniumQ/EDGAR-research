import logging
import datetime
import contextlib


@contextlib.contextmanager
def time_code(msg=None):
    dct = {}
    start = datetime.datetime.now()
    logging.info(msg)
    try:
        yield dct
    finally:
        stop = datetime.datetime.now()
        time = stop - start
        dct['time'] = time
        if msg:
            seconds = time.total_seconds()
            logging.info(f'{msg}: {seconds:.3f}s')


def flatten1(list_):
    return [elem for list2 in list1 for elem in list2]
