import os
import os.path

RESULTS_PARENT_DIR = 'results'

def new_directory(make=True):
    if not os.path.exists(RESULTS_PARENT_DIR):
        os.mkdir(RESULTS_PARENT_DIR)
    directory = os.path.join(RESULTS_PARENT_DIR, 'result_')
    i = 99
    i_s = '{:02d}'.format(i)
    while os.path.isdir(directory + i_s) or os.path.isfile(directory + i_s):
        i -= 1
        i_s = '{:02d}'.format(i)
    directory += i_s
    if make:
        os.mkdir(directory)
    return directory + os.path.sep

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
