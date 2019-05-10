from .time_code import time_code

# from .fs_util import (
#     rand_names,
#     sanitize_unused_fname,
#     new_directory,
# )


def download_retry(url, max_retries=10, cooldown=5):
    import urllib
    import time
    for retry in range(max_retries):
        try:
            return urllib.request.urlopen(url).read()
        except Exception as exc: # pylint: disable=broad-except
            if retry == max_retries - 1:
                raise exc
            else:
                time.sleep(cooldown)


class Struct: # pylint: disable=too-few-public-methods
    pass


def generator_to_list(generator):
    def func(*args, **kwargs):
        return list(generator(*args, **kwargs))
    return func


def invert(dct):
    return {value: key for key, value in dct.items()}


# def dicts2csr(dicts, width=0):
#     from scipy.sparse import csr_matrix
#     indptr = [0]
#     indices = []
#     data = []
#     for dct in dicts:
#         for ind, val in dct.items():
#             width = max(width, ind)
#             indices.append(ind)
#             data.append(val)
#         indptr.append(len(indices))
#     return scipy.sparse.csr_matrix((data, indices, indptr), shape)


def generator2iterator(generator, length=None):
    class Iterator:
        def __init__(self):
            self.length = length
        def __iter__(self):
            if self.length is not None:
                yield from iter(generator())
            else:
                length = 0
                for elem in iter(generator()):
                    length += 1
                    yield elem
                self.length = length
        def __len__(self):
            if self.length is not None:
                return self.length
            else:
                raise RuntimeError('len not known yet')
    return Iterator()

def concat_lists(lists):
    ret = []
    for list_ in lists:
        ret.extend(list_)
    return ret

def merge_dicts(dicts):
    ret = {}
    for dict_ in dicts:
        ret.update(dict_)
    return ret

# import traceback
# import signal
# import sys
# import os
# import time
# import random
# def handle_signal(signal, frame):
#     for thread, frame in sys._current_frames().items():
#         # random sleeps helps prevent stacktraces from different threads being interleaved
#         time.sleep(random.random() * 0.1)
#         # this prints the entire stacktrace without intermediate buffering
#         print(''.join(traceback.format_stack(frame)) + '\n')
# signal.signal(signal.SIGUSR1, handle_signal)
# print(f'For stacktrace: kill -s SIGUSR1 {os.getpid()}', file=sys.stderr)
