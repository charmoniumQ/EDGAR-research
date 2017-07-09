import functools
import operator

def flatten(lst):
    return functools.reduce(operator.add, lst, [])
