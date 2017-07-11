def project(dct, keys):
    return {key: val for key, val in dct.items() if key in keys}

