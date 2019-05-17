from typing import Callable, Any, TypeVar
from pathlib import Path
import urllib.request
import time
import tempfile
import logging
import logging.config
import shutil
import os
import dask.distributed
from edgar_code.cache import Cache
from edgar_code.bag_store import BagStore
from edgar_code.gs_path import GSPath


logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'main_formatter': {
            'format': '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        },
    },
    'handlers': {
        'main_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'main_formatter',
        },
    },
    'root': {
        'handlers': [
            'main_handler',
        ],
        'level': 'DEBUG',
    },
})

cred_path = Path('./service_account.json')
if cred_path.exists():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(cred_path)

cache_path = GSPath.from_url('gs://results-7/cache2')

results_path = GSPath.from_url('gs://results-7/results2')

cache = True

FuncType = TypeVar('FuncType', bound=Callable[..., Any])
cache_decor = Cache.decor(BagStore.create(cache_path), miss_msg=True)

def get_client() -> dask.distributed.Client:
    address = os.environ.get('SCHEDULER_PORT', None)
    if address:
        logging.info('using remote scheduler: %s', address)

    for _ in range(10):
        try:
            client = dask.distributed.Client(address=address)
        except OSError:
            continue
        else:
            break
    else:
        # try > else > break was never hit
        raise ValueError('Could not connect to scheduler')


    n_workers_ = os.environ.get('N_WORKERS', None)
    if n_workers_ is not None:
        n_workers = int(n_workers_)
        logging.info('waiting for %d workers', n_workers)
        client.wait_for_workers(n_workers)

    egg_url = os.environ.get('DEPLOY_EGG', None)
    if egg_url is not None:
        logging.info('Downloading and uploading egg: %s', egg_url)
        with tempfile.TemporaryDirectory() as tempdir:
            egg_path = Path(tempdir) / 'code.egg'
            src = urllib.request.urlopen(egg_url)
            with egg_path.open('wb') as dst:
                shutil.copyfileobj(src, dst)
            client.upload_file(str(egg_path))

    return client
