import itertools
from pathlib import Path

RESULTS = Path('results')

def new_directory():
    if not RESULTS.exists():
        RESULTS.mkdir()

    # count down from 99
    for i in range(99, -1, -1):
        directory = RESULTS / 'result_{:02d}'.format(i)
        if not directory.exists():
            break
    else:
        # count up from 100 if 0-99 already taken
        for i in itertools.count(100):
            directory = RESULTS / 'result_{:02d}'.format(i)
            if not directory.exists():
                break
    directory.mkdir()
    return directory

import urllib.request
import random
def get_name():
    if not hasattr(get_name, 'nouns'):
        nouns_url = 'https://raw.githubusercontent.com/polleverywhere/random_username/master/lib/random_username/nouns.txt'
        get_name.nouns = list(filter(bool, urllib.request.urlopen(nouns_url).read().decode().split('\n')))
    if not hasattr(get_name, 'adjs'):
        adjs_url = 'https://raw.githubusercontent.com/polleverywhere/random_username/master/lib/random_username/adjectives.txt'
        get_name.adjs = list(filter(bool, urllib.request.urlopen(adjs_url  ).read().decode().split('\n')))
    return random.choice(get_name.adjs), random.choice(get_name.nouns)
