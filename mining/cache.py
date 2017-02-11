from __future__ import print_function
from six.moves.urllib.request import urlopen
from os import mkdir
from os.path import join, isdir, isfile

CACHE_DIR = 'mining/edgar-downloads'
ENABLE_CACHING = True
VERBOSE = True

def download(path, enable_cache=True):
    '''Download a copy of a file and cache it.
    If the file has already been downloaded into the cache, use that instead.
    You are responsible for closing the file.'''
    try:
        return get(path, enable_cache)
    except NotFound:
        file = download_no_cache(path)
        if enable_cache and ENABLE_CACHING:
            put(path, file, enable_cache)
            return get(path, enable_cache)
        else:
            return file

def download_no_cache(path):
    '''Download a file without attempting to read or write from the cache'''
    if VERBOSE: print('cache.py: downloading {path}'.format(**locals()))
    url_path = 'https://www.sec.gov/Archives/' + path
    while True:
        try:
            raw_file = urlopen(url_path.format(**locals())).read()
        except:
           if VERBOSE: print('cache.py: retrying')
        else:
            break
    if VERBOSE: print('cache.py: done        {path}'.format(**locals()))
    return raw_file

def _normalize(path):
    if not isdir(CACHE_DIR):
        mkdir(CACHE_DIR)
    return join(CACHE_DIR, path.replace('/', '__'))    

def get(path, enable_cache=True):
    '''Attempt to retrieve file from cache, raising NotFound if not found.
    You are responseible for closing the file, if it is returned'''
    cache_path = _normalize(path)
    if enable_cache and ENABLE_CACHING and isfile(cache_path):
        if VERBOSE: print('cache.py: retrieving  {path}'.format(**locals()))
        file = open(cache_path, 'rb')
        contents = file.read()
        file.close()
        return contents
    else:
        raise NotFound('Unable to find {path}'.format(**locals()))

def put(path, file, enable_cache=True):
    '''Store file in the cache for path'''
    if enable_cache and ENABLE_CACHING:
        if VERBOSE: print('cache.py: storing     {path}'.format(**locals()))
        cache_path = _normalize(path)
        with open(cache_path, 'wb') as outfile:
            if isinstance(file, (str, bytes)):
                outfile.write(file)
            else:
                for line in file:
                    outfile.write(line)
        file.close()

class NotFound(Exception):
    pass
