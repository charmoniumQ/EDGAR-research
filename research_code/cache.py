from __future__ import print_function
from six.moves.urllib.request import urlopen
from pathlib import Path

CACHE_DIR = Path('results/edgar-downloads')

def download(path, enable_cache, verbose=False):
    '''Download a copy of a file and cache it.
    If the file has already been downloaded into the cache, use that instead.
    You are responsible for closing the file.'''
    try:
        return get(path, enable_cache, verbose)
    except NotFound:
        file = download_no_cache(path, verbose)
        if enable_cache:
            put(path, file, enable_cache, verbose)
            return get(path, enable_cache, verbose)
        else:
            return file

def download_no_cache(path, verbose):
    '''Download a file without attempting to read or write from the cache'''
    if verbose: print('cache.py: downloading {path}'.format(**locals()))
    url_path = 'https://www.sec.gov/Archives/' + path
    while True:
        try:
            raw_file = urlopen(url_path.format(**locals())).read()
        except:
           if verbose: print('cache.py: retrying')
        else:
            break
    if verbose: print('cache.py: done        {path}'.format(**locals()))
    return raw_file

def _normalize(path):
    if not CACHE_DIR.is_dir():
        CACHE_DIR.mkdir(parents=True)
    return CACHE_DIR / path.replace('/', '__')

def get(path, enable_cache, verbose):
    '''Attempt to retrieve file from cache, raising NotFound if not found.
    You are responseible for closing the file, if it is returned'''
    if enable_cache:
        cache_path = _normalize(path)
        if cache_path.is_file():
            if verbose: print('cache.py: retrieving  {path}'.format(**locals()))
            file = cache_path.open('rb')
            contents = file.read()
            file.close()
            return contents
        else:
            raise NotFound('Unable to find {path}'.format(**locals()))                
    else:
        raise NotFound('Unable to find {path}'.format(**locals()))

def put(path, file, enable_cache, verbose):
    '''Store file in the cache for path'''
    if enable_cache:
        if verbose: print('cache.py: storing     {path}'.format(**locals()))
        cache_path = _normalize(path)
        with cache_path.open('wb') as outfile:
            if isinstance(file, (str, bytes)):
                outfile.write(file)
            else:
                for line in file:
                    outfile.write(line)
        if not isinstance(file, (str, bytes)):
            file.close()

class NotFound(Exception):
    pass

__all__ = ['download']
