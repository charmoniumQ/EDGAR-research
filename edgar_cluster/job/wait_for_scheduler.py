#!/usr/bin/env python3

import os
import logging
logging.basicConfig(level=logging.INFO)
import google.api_core.exceptions
import google.cloud.storage
import time


bucket_name = os.environ['google_storage_bucket']
storage_client = google.cloud.storage.Client()
bucket = storage_client.get_bucket(bucket_name)
status_blob = bucket.blob('status')


while True:
    try:
        status = status_blob.download_as_string().decode()
    except google.cloud.exceptions.NotFound:
        status = ''
    logging.info(f'status = {status}')
    if status == 'done':
        break
    time.sleep(3)
