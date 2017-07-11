import itertools
import random


def random_sample(it, p, max=None):
    if max is not None:
        yield from itertools.islice(random_sample(it, p), 0, max)
    else:
        for elem in it:
            if random.random() < p:
                yield it
