import os
import google.cloud.storage
import asyncio

class BucketWait(object):
    def __init__(bucket_name):
        self.storage_client = google.cloud.storage.Client()
        self.bucket = self.storage_client.get_bucket(bucket_name)

    def trigger(self, key, value=b'done'):
        value_blob = self.bucket.blob(f'wait/{key!s}')
        value_blob.upload_from_string(key, value)

    async def wait(self, key, value_check=None, delay=4):
        value_blob = self.bucket.blob(f'wait/{key!s}')
        while True:
            try:
                value = value_blob.download_as_string().decode()
            except google.cloud.exceptions.NotFound:
                value = ''
            if value is None or value_check(value):
                break
            await asyncio.sleep(delay)
        return value
