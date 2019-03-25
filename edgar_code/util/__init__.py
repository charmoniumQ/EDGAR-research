from .fs_util import (
    rand_names,
    sanitize_unused_fname,
    new_directory,
)
from . import cache


class Struct(object):
    pass


def generator_to_list(f):
    def f_(*args, **kwargs):
        return list(f(*args, **kwargs))
    return f_
