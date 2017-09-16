import datetime
import contextlib

time = -1

@contextlib.contextmanager
def timer(print_=False):
    start = datetime.datetime.now()
    try:
        yield
    finally:
        stop = datetime.datetime.now()
        global time; time = (stop - start).total_seconds()
        if print_:
            print('Took {:.1f}s'.format(time))
