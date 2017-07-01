def project(dct, keys):
    if keys is None:
        keys = dct.keys()
    return {key: val for key, val in dct.items()
            if key in keys}


def key_val(keys=None, vals=None):
    def key_val_(record):
        return (project(record, keys), project(record, vals))


def key(pair):
    return pair[0]


def val(pair):
    return pair[1]
