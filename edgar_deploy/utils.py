class Struct(object):
    pass


def omit(dct, keys):
    '''omit keys from a dict'''
    return {key: val for key, val in dct.items() if key not in keys}


def flatten_iter(it, level=0):
    '''Lazily flattens an interator of iterators of ... of iterators
Yields (level, item) where item is a string or non-iterable type.'''
    for item in it:
        if isinstance(item, str) or not hasattr(item, '__next__'):
            yield level, item
        else:
            yield from flatten_iter(item, level+1)


def flatten1(list_):
    '''[[X]] -> [X]'''
    return [elem for list2 in list1 for elem in list2]


def empty_iter(itera):
    '''Test if an iterator is empty'''
    itera = iter(itera)
    try:
        itera.next()
    except Stopiteration:
        return True
    else:
        return False


import time
def exp_backoff(initial, exp, max_):
    delay = initial
    while True:
        yield
        logging.info(f'delaying {delay:.1f}s')
        time.sleep(delay)
        delay = min(delay * exp, max_)


import datetime
unix_epoch = datetime.datetime(year=1970, month=1, day=1)
def timestamp_to_datetime(timestamp):
    return unix_epoch + datetime.timedelta(seconds=timestamp)


def datetime_to_datetime(datetime_obj):
    return (datetime_obj - unix_epoch).total_seconds()


import random
import string
def rand_name(n=10, lowercase=False, uppercase=False, digits=False, upperhexdigits=False, lowerhexdigits=False, custom_alphabet=''):
    alphabet = ''
    if lowercase:
        alphabet += string.ascii_lowercase
    if uppercase:
        alphabet += string.ascii_uppercase
    if digits:
        alphabet += string.digits
    if upperhexdigits:
        alphabet += '0123456789ABCDEF'
    if lowerhexdigits:
        alphabet += '0123456789abcdef'
    if custom_alphabet:
        alphabet += custom_alphabet
    alphabet = list(set(alphabet))
    return ''.join(random.choice(alphabet) for _ in range(n))
