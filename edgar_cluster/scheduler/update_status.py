#!/usr/bin/env python3

import os
import sys
import logging
logging.basicConfig(level=logging.INFO)
import google.api_core.exceptions
import google.cloud.storage
import re

bucket_name = os.environ['google_storage_bucket']
storage_client = google.cloud.storage.Client()
bucket = storage_client.get_bucket(bucket_name)
status_blob = bucket.blob('status')
n_workers = int(os.environ['n_workers'])


registered_n_workers = 0
add_worker = re.compile('Starting worker compute stream')


for line in sys.stdin:
    print(line, end='')
    sys.stdout.flush()
    if add_worker.search(line):
        registered_n_workers += 1
        logging.info(f'registered another worker (x{registered_n_workers})')
        if n_workers == registered_n_workers:
            logging.info(f'all workers acquired')
            status_blob.upload_from_string('done')
