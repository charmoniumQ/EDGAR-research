#!/usr/bin/env python3
import os
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './edgar_deploy/main-722.service_account.json'
import sys
import traceback
import google.api_core.exceptions
import google.cloud.pubsub_v1
import logging
logging.basicConfig(level=logging.INFO)

gcloud_project = os.environ['gcloud_project']
gcloud_topic = os.environ['gcloud_topic']
gcloud_subscription = os.environ['gcloud_subscription']

# https://googleapis.github.io/google-cloud-python/latest/pubsub/#subscribing
subscriber = google.cloud.pubsub_v1.SubscriberClient()
topic_name = f'projects/{gcloud_project}/topics/{gcloud_topic}'
subscription_name = f'projects/{gcloud_project}/subscriptions/{gcloud_subscription}'

logging.info(topic_name)
logging.info(subscription_name)
import yaml
with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) as f:
    logging.info(yaml.load(f)["client_email"])

try:
    subscriber.create_subscription(name=subscription_name, topic=topic_name)
except google.api_core.exceptions.AlreadyExists:
    pass

def callback(message):
    print(message.data)
    message.ack()

future = subscriber.subscribe(subscription_name, callback)

try:
    logging.info('waiting for scheduler ready')
    future.result()
    logging.info('scheduler ready')
    sys.stdout.flush()
except:
    future.cancel()
    traceback.print_exc()
    sys.stdout.flush()
