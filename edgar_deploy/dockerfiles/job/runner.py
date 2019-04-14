#!/usr/bin/env python3
import os
import google.cloud.storage
import importlib
import dask.distributed
import sys


scheduler_address = os.environ['dask_scheduler_address']
bucket_name = os.environ['google_storage_bucket']
run_module = os.environ['run_module']
namespace = os.environ['namespace']


dask_client = dask.distributed.Client(scheduler_address)


# download and install egg
remote_egg_path = f'eggs/edgar_code-{namespace}.egg'
local_egg_path = '/tmp/edgar_code.egg'
storage_client = google.cloud.storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(remote_egg_path)
blob.download_to_filename(local_egg_path)
dask_client.upload_file(local_egg_path)


sys.path.insert(0, local_egg_path)


importlib.import_module(run_module).main()
