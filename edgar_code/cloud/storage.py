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
# GOOGLE_APPLICATION_CREDENTIALS=edgar_deploy/main-722.service_account.json python3 -m edgar_code.executables.get_all_rfs

client = google.cloud.storage.Client()


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
        self.path = Path(path)
        if isinstance(bucket, str):
            bucket = client.bucket(bucket)
        self.bucket = bucket
        self.blob = self.bucket.blob(str(self.path))

    def __getstate__(self):
        return {'path': self.path, 'bucket': self.bucket.name}

    def __setstate__(self, data):
        self.path = data['path']
        self.bucket = client.bucket(data['bucket'])
        self.blob = self.bucket.blob(str(self.path))

    def __truediv__(self, other):
        return GSPath(self.bucket, self.path / other)

    def __repr__(self):
        url = f'gs://{self.bucket.name}/{self.path}'
        return f'GSPath.from_url({url!r})'

    @property
    def parent(self):
        return GSPath(self.bucket, self.path.parent)

    def mkdir(self, exist_ok=True, parents=True):
        # no notion of 'directories' in GS
        pass

    def rmtree(self):
        for path in self.iterdir():
            path.unlink()

    def exists(self):
        # print(f'{self.blob.name} exists? {self.blob.exists()}')
        return self.blob.exists()

    def unlink(self):
        self.blob.delete()

    def iterdir(self):
        for blob in self.bucket.list_blobs(prefix=f'{self.path!s}/'):
            yield GSPath.from_blob(blob)

    def open(self, flags, encoding='utf-8'):
        if 'w' in flags:
            if 'b' in flags:
                return WGSFile(self, flags)
            else:
                return io.TextIOWrapper(WGSFile(self, flags), encoding=encoding, errors='strict')
        elif 'r' in flags:
            import time
            if 'b' in flags:
                for i in range(10):
                    try:
                        return io.BytesIO(self.blob.download_as_string())
                    except Exception as e:
                        if i == 9:
                            raise e
                        else:
                            time.sleep(6)
                    
            else:
                for i in range(10):
                    try:
                        return io.BytesIO(self.blob.download_as_string().decode())
                    except Exception as e:
                        if i == 9:
                            raise e
                        else:
                            time.sleep(6)
        else:
            raise RuntimeError(f'Flag {flags} not supported')


class WGSFile(io.BytesIO):
    def __init__(self, gs_path: GSPath, flags: str):
        self.gs_path = gs_path

    def close(self):
        self.gs_path.blob.upload_from_file(self, rewind=True)
        super().close()


from pathlib import Path
def pathify(path):
    if isinstance(path, str):
        return Path(path)
    else:
        # assume already path-y
        return path


def copy(in_path, out_path):
    with pathify(in_path).open('rb') as fin:
        with pathify(out_path).open('wb') as fout:
            fout.write(fin.read())
