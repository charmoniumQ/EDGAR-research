from . import utils
from .config import config
from .provisioned_resource import FileProvisionedResource
import contextlib
import google.cloud.storage
# https://googleapis.github.io/google-cloud-python/latest/storage/client.html


class S3Bucket(FileProvisionedResource):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.client = google.cloud.storage.Client()
        self.bucket = self.client.bucket(self.name)
        self.bucket.storage_class = "REGIONAL"
        self.bucket.create(location=config.gcloud.region)
        super().__init__(*args, **kwargs, name=name)

    def delete(self):
        self.bucket.delete(force=True)
        super().delete()

    def __getstate__(self):
        return {name: self.name}

    def __setstate__(self, state):
        self.name = state['name']
        self.client = google.cloud.storage.Client()
        self.bucket = self.client.create_bucket(self.name)
