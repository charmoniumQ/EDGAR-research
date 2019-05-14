from typing import Callable, Any, TypeVar
from pathlib import Path
import time
import logging
import os
import dask.distributed
from edgar_code.cache import Cache
from edgar_code.bag_store import BagStore
from edgar_code.gs_path import GSPath


# TODO: research how this setting propagates to other modules
logging.basicConfig(level=logging.INFO)

cred_path = Path('./service_account.json')
if cred_path.exists():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(cred_path)

cache_path = GSPath.from_url('gs://results-7/cache')

results_path = GSPath.from_url('gs://results-7/results')

cache = True

FuncType = TypeVar('FuncType', bound=Callable[..., Any])
def get_cache_decor() -> Callable[[FuncType], FuncType]:
    if cache:
        return Cache.decor(
            BagStore.create(cache_path / 'bags'), miss_msg=True,
        )
    else:
        return lambda x: x

def get_client() -> dask.distributed.Client:
    address = os.environ.get('SCHEDULER_PORT', None)
    if address:
        logging.info('found scheduler address: %s', address)

    client = dask.distributed.Client(address=address)

    n_workers_ = os.environ.get('N_WORKERS', None)
    if n_workers_:
        n_workers = int(n_workers_)
        actual_n_workers = len(client.scheduler_info()['workers'])
        while actual_n_workers < n_workers:
            logging.info('%d/%d workers', actual_n_workers, n_workers)
            time.sleep(1)
        logging.info('got all %d workers', actual_n_workers)

    egg = os.environ.get('DEPLOY_EGG', None)
    if egg:
        logging.info('uploading egg: %s', egg)
        client.upload_file(egg)

    return client
