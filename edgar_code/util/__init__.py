from typing import TYPE_CHECKING
from edgar_code.util.time_code import time_code
from edgar_code.util.list_dict import (
    merge_dicts, concat_lists, generator2iterator, invert,
    generator2fn_list, flatten_iter
)
import edgar_code.util.picklable_threading


def download_retry(
        url: str, max_retries: int = 10, cooldown: float = 5
) -> bytes:
    import urllib.request
    import time
    for retry in range(max_retries):
        try:
            return urllib.request.urlopen(url).read()
        except Exception as exc: # pylint: disable=broad-except
            if retry == max_retries - 1:
                raise exc
            else:
                time.sleep(cooldown)
    raise RuntimeError('unreachable code')


class Struct:
    pass


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
