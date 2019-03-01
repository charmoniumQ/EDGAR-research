#!/usr/bin/env python3

import os
import sys
import re
import logging
logging.basicConfig(level=logging.INFO)
import traceback
import google.api_core.exceptions
import google.cloud.pubsub_v1

gcloud_project = os.environ['gcloud_project']
gcloud_topic = os.environ['gcloud_topic']
n_workers = int(os.environ['n_workers'])

# https://googleapis.github.io/google-cloud-python/latest/pubsub/#publishing
publisher = google.cloud.pubsub_v1.PublisherClient()
topic_name = f'projects/{gcloud_project}/topics/{gcloud_topic}'
try:
    publisher.create_topic(topic_name)
except google.api_core.exceptions.AlreadyExists:
    pass

actual_n_workers = 0

add_worker = re.compile('Starting worker compute stream')

for line in sys.stdin:
    print(line, end='')
    sys.stdout.flush()

    if add_worker.search(line):
        actual_n_workers += 1
        logging.info(f'workers = {actual_n_workers}')
        if n_workers == actual_n_workers:
            logging.info(f'telling pubsub')
            future = publisher.publish(topic_name, 'scheduler ready'.encode())
            logging.info(f'told pubsub')
            future.result()
            logging.info(f'pubsub heard me')
