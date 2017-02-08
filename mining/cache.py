from __future__ import print_function
from urllib.request import urlopen
from os import mkdir
from os.path import join, isdir, isfile

CACHE_DIR = 'mining/edgar-downloads'
ENABLE_CACHING = True

def download(path):
    '''Download a copy of a file and cache it.
    If the file has already been downloaded into the cache, use that instead.
    You are responsible for closing the file.'''
    try:
        return get(path)
    except NotFound:
        put(path, download_no_cache(path))
        return get(path)

def download_no_cache(path):
    '''Download a file without attempting to read or write from the cache'''
    print('cache.py: downloading {path}'.format(**locals()))
    url_path = 'https://www.sec.gov/Archives/' + path
    download = False
    while not download:
        try:
             raw_file = urlopen(url_path.format(**locals())).read()
             download = True
        except:
            download = False
    print('cache.py: done downloading {path}'.format(**locals()))
    return raw_file

def _normalize(path):
    if not isdir(CACHE_DIR):
        mkdir(CACHE_DIR)
    return join(CACHE_DIR, path.replace('/', '__'))    

def get(path):
    '''Attempt to retrieve file from cache, raising NotFound if not found.
    You are responseible for closing the file, if it is returned'''
    cache_path = _normalize(path)
    if ENABLE_CACHING and isfile(cache_path):
        return open(cache_path, 'rb')
    else:
        raise NotFound('Unable to find {path}'.format(**locals()))

def put(path, file):
    '''Store file in the cache for path'''
    if ENABLE_CACHING:
        cache_path = _normalize(path)
        with open(cache_path, 'wb') as outfile:
            if isinstance(file, (str, bytes)):
                outfile.write(file)
            else:
                for line in file:
                    outfile.write(line)

class NotFound(Exception):
    pass
