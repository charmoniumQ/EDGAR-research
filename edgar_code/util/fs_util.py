import itertools
from pathlib import Path
import re
import urllib.request
import random
import string


RESULTS = Path('results')


def sanitize_fname(fname):
    fname = re.sub(r'[^a-zA-Z0-9_-]', '_', fname)
    fname = re.sub(r'_{2,}', '_', fname)
    fname = re.sub(r'_$', '', fname)
    return fname


def unused_fname(dire, fname, suffix):
    # TODO: make this more general, eliminate duplicated code

    def candidates():
        # try just dire/fname
        yield dire / fname
        for n in itertools.count(1):
            # otherwise add n
            yield dire / f'{fname}_{n}.{suffix}'

    for try_fname in candidates():
        if not try_fname.exists():
            return try_fname


def sanitize_unused_fname(dir_, name, suffix):
    starting_fname = sanitize_fname(name)
    fname = unused_fname(dir_, starting_fname, suffix)
    return fname

def new_directory(verbose=True):
    if not RESULTS.exists():
        RESULTS.mkdir()

    # count down from 99
    for i in itertools.chain(range(99, -1, -1), itertools.count(100)):
        directory = RESULTS / 'result_{:02d}'.format(i)
        if not directory.exists():
            break
    directory.mkdir()
    if verbose:
        print('results in {!s}'.format(directory))
    return directory


def rand_word_name():
    if not hasattr(get_name, 'nouns'):
        nouns_url = 'https://raw.githubusercontent.com/polleverywhere/random_username/master/lib/random_username/nouns.txt'
        get_name.nouns = list(filter(bool, urllib.request.urlopen(nouns_url).read().decode().split('\n')))
    if not hasattr(get_name, 'adjs'):
        adjs_url = 'https://raw.githubusercontent.com/polleverywhere/random_username/master/lib/random_username/adjectives.txt'
        get_name.adjs = list(filter(bool, urllib.request.urlopen(adjs_url  ).read().decode().split('\n')))
    return random.choice(get_name.adjs), random.choice(get_name.nouns)


def find_file(filename, paths):
    for path in paths:
        if (path / filename).exists():
            return path / filename


def rand_name(n):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))


def rand_names(n):
    while True:
        yield ''.join(random.choice(string.ascii_lowercase) for _ in range(n))
