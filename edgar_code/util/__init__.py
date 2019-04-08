from .fs_util import (
    rand_names,
    sanitize_unused_fname,
    new_directory,
)
from . import cache


def download_retry(url, max_retries=10, cooldown=5):
    import urllib
    import time
    for retry in range(max_retries):
        try:
            return urllib.request.urlopen(url).read()
        except Exception as e:
            if retry == max_retries - 1:
                raise e
            else:
                time.sleep(cooldown)


class Struct(object):
    pass


def generator_to_list(f):
    def f_(*args, **kwargs):
        return list(f(*args, **kwargs))
    return f_
