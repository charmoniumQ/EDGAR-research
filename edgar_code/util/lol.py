import itertools
from toolz.functoolz import curry

# List of List utilities


def llist(lst):
    '''Turns nested generators into nested lists.'''
    # special case for strings, because when you iterate through strings,
    # each element is also a string, but you shouldn't iterate through the
    # characters of each string or else you would have infinite recursion.
    if (hasattr(lst, '__next__') or hasattr(lst, '__iter__')) \
       and not isinstance(lst, str):
        return list(map(llist, lst))
    else:
        return lst


def lmap(f, elem, level):
    if level == 0:
        return f(elem)
    else:
        f_prime = curry(lmap)(f, level=level-1)
        return map(f_prime, elem)


def flatten(lst):
    '''Turns a list of list of list...(etc.) into just a flat list'''
    if (hasattr(lst, '__next__') or hasattr(lst, '__iter__')) \
       and not isinstance(lst, str):
        for e in lst:
            yield from flatten(e)
    else:
        # print('found elem', lst)
        yield lst


def test_llist():
    assert llist([range(3), [iter(['def'])]]) == [[0, 1, 2], [['def']]]


def test_lmap():
    o1 = [range(3)       , range(3)      ]
    o2 = [['0', '1', '2'], ['0', '1', '2']]
    assert llist(lmap(str, o1, 2)) == o2


def test_flatten():
    o1 = [[range(3)], ['hello'], 2]
    o2 = [ 0, 1, 2,    'hello',  2]
    assert list(flatten(o1)) == o2


if __name__ == '__main__':
    test_llist()
    test_lmap()
    test_flatten()
