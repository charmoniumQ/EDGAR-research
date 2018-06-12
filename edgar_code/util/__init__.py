from .fs_util import (
    sanitize_unused_fname,
    new_directory,
    find_file,
    rand_name,
    BOX_PATH,
    rand_names
)
from . import cache


def generator_to_list(f):
    def f_(*args, **kwargs):
        return list(f(*args, **kwargs))
    return f_
