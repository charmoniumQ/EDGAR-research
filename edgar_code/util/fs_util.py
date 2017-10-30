import itertools
from pathlib import Path
import re
import urllib.request
import random
import string


BOX_PATH = [Path('../Box Sync/EDGAR Team'), Path('~/Box Sync/EDGAR Team').expanduser()]
RESULTS = Path('results')


def sanitize_fname(fname):
    return re.sub(r'[^a-zA-Z\.-_]', '', fname)


def unused_fname(dire, fname):
    # TODO: make this more general, eliminate duplicated code

    def candidates():
        yield dire / fname  # try just dire/fname
        for n in itertools.count(1):
            yield dire / f'{fname}_{n}'  # otherwise add n

    for try_fname in candidates():
        if not try_fname.exists():
            return try_fname


def new_directory():
    if not RESULTS.exists():
        RESULTS.mkdir()

    # count down from 99
    for i in itertools.chain(range(99, -1, -1), itertools.count(100)):
        directory = RESULTS / 'result_{:02d}'.format(i)
        if not directory.exists():
            break
    directory.mkdir()
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
