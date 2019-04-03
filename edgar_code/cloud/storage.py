import collections
from urllib.parse import urlparse
import io
import sys
if sys.version_info >= (3, 7):
    from dataclasses import dataclass
else:
    dataclass = lambda: lambda y: y
from pathlib import Path
import google.cloud.storage
# https://googleapis.github.io/google-cloud-python/latest/storage/client.html

# TODO: config
# import os
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'edgar_deploy/main-722.service_account.json'


@dataclass()
class GSPath(object):
    path: Path
    bucket: google.cloud.storage.bucket.Bucket
    blob: google.cloud.storage.blob.Blob

    @classmethod
    def from_blob(Class, blob):
        return Class(blob.bucket, blob.name)

    @classmethod
    def from_url(Class, url_str):
        url = urlparse(url_str)
        if url.scheme != 'gs':
            raise ValueError('Wrong url scheme')
        return Class(url.netloc, url.path[1:])

    def __init__(self, bucket, path):
        self.client = google.cloud.storage.Client()
        self.path = Path(path)
        if isinstance(bucket, str):
            bucket = self.client.bucket(bucket)
        self.bucket = bucket
        self.blob = self.bucket.blob(str(self.path))

    def __getstate__(self):
        return {'path': self.path, 'bucket': self.bucket.name}

    def __setstate__(self, data):
        self.client = google.cloud.storage.Client()
        self.path = data['path']
        self.bucket = self.client.bucket(data['bucket'])
        self.blob = self.bucket.blob(str(self.path))

    def __truediv__(self, other):
        return GSPath(self.bucket, self.path / other)

    def __repr__(self):
        return f'GSPath.from_url(gs://{self.bucket}/{self.path})'

    @property
    def parent(self):
        return GSPath(self.bucket, self.path.parent)

    def mkdir(self, exist_ok=True, parents=True):
        # no notion of 'directories' in GS
        pass

    def exists(self):
        return self.blob.exists()

    def unlink(self):
        self.blob.delete()

    def iterdir(self):
        for blob in self.bucket.list_blobs(prefix='{self.path!s}/'):
            yield GSPath.from_blob(blob.name)

    def open(self, flags):
        return GSFile(self)


class GSFile(io.BytesIO):
    def __init__(self, gs_path: GSPath):
        self.gs_path = gs_path

    def close(self):
        self.gs_path.blob.upload_from_file(self, rewind=True)
        super().close()
