from typing import Any
import datetime
import importlib
import collections
import msgpack


def encode_obj(obj: Any) -> Any:
    if isinstance(obj, datetime.date):
        return dict(
            py_expr='datetime.datetime.strptime(data, "%Y-%m-%d").date()',
            data=datetime.date.strftime(obj, '%Y-%m-%d'),
        )
    elif isinstance(obj, set):
        return dict(
            py_expr='set(data)',
            data=list(obj)
        )
    elif isinstance(obj, Exception):
        return dict(
            py_expr=f'{type(obj).__name__}(data)',
            data=str(obj),
        )
    elif isinstance(obj, collections.Counter):
        return dict(
            py_expr='collections.Counter(data)',
            data=dict(obj),
        )
    elif hasattr(obj, '_make') and hasattr(obj, '_fields'):
        package, _, _ = obj.__module__.rpartition('.')
        # we have a namedtuple
        return dict(
            py_expr=(
                f'importlib.import_module({obj.__module__!r}, {package!r})'
                f'.{type(obj).__name__}(**data)'
            ),
            data=dict(obj._asdict()),
        )
    elif isinstance(obj, tuple):
        return dict(
            py_expr='tuple(data)',
            data=list(obj),
        )
    else:
        return obj


def decode_obj(obj: Any) -> Any:
    if isinstance(obj, dict) and 'py_expr' in obj:
        return eval( # pylint: disable=eval-used
            obj['py_expr'],
            {**locals(), 'data': obj['data']},
            {**globals(), 'importlib': importlib},
        )
    else:
        return obj


def dump(obj: Any, fil: Any) -> None:
    return msgpack.dump(obj, fil, use_bin_type=True,
                        default=encode_obj, strict_types=True)
def dumps(obj: Any) -> bytes:
    return msgpack.dumps(obj, use_bin_type=True,
                         default=encode_obj, strict_types=True)
def loads(buf: bytes) -> Any:
    return msgpack.loads(buf, raw=False, object_hook=decode_obj,
                         use_list=True)
def load(fil: Any) -> Any:
    return msgpack.load(fil, raw=False, object_hook=decode_obj,
                        use_list=True)
