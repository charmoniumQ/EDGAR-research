
def project(dct, keys):
    '''Returns a dct with only the given keys'''
    return {key: val for key, val in dct.items() if key in keys}

def without(dct, keys):
    '''Returns a dct without the given keys'''
    return {key: val for key, val in dct.items() if key not in keys}    
