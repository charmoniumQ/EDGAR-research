from . import utils


def modtime(path):
    s = path.stat()
    latest_timestamp = max(s.st_mtime, s.st_ctime)
    return utils.timestamp_to_datetime(latest_timestamp)


def modtime_recursive(path):
    if path.is_dir():
        candidates = [modtime(path)] + [modtime_recursive(entry) for entry in path.iterdir()]
        return max(candidates)
    else:
        return modtime(path)

