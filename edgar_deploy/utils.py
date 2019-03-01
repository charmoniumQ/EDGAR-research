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


def flatten_gen(gen, level=0):
    for item in gen:
        if isinstance(item, str) or not hasattr(item, '__next__'):
            yield level, item
        else:
            yield from flatten_gen(item, level+1)

def flatten1(list_):
    return [elem for list2 in list1 for elem in list2]
