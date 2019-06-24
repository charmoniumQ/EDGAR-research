from typing import Callable, Any, TypeVar
from pathlib import Path
import urllib.request
import tempfile
import logging
import logging.config
import shutil
import os
import distributed
import yaml
from edgar_code.cache import Cache
from edgar_code.bag_store import BagStore
from edgar_code.gs_path import GSPath


project_root = Path(__file__).parent.parent

with (project_root / 'logging.yaml').open() as fobj:
    logging.config.dictConfig(yaml.load(fobj, yaml.SafeLoader))

cred_path = Path('./service_account.json')
if cred_path.exists():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(cred_path)

cache_path = GSPath.from_url('gs://results-7/cache2')

results_path = GSPath.from_url('gs://results-7/results2')

cache = True

FuncType = TypeVar('FuncType', bound=Callable[..., Any])
cache_decor = Cache.decor(BagStore.create(cache_path), miss_msg=True)

def get_client() -> distributed.Client:
    address = os.environ.get('SCHEDULER_PORT', None)

    for _ in range(10):
        try:
            client = distributed.Client(address=address)
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
        logging.debug('Waiting for: %d workers', n_workers)
        client.wait_for_workers(n_workers)

    egg_url = os.environ.get('DEPLOY_EGG', None)
    if egg_url is not None:
        logging.debug('Downloading and uploading egg: %s', egg_url)
        with tempfile.TemporaryDirectory() as tempdir:
            egg_path = Path(tempdir) / 'code.egg'
            src = urllib.request.urlopen(egg_url)
            with egg_path.open('wb') as dst:
                shutil.copyfileobj(src, dst)
            client.upload_file(str(egg_path))

    return client
