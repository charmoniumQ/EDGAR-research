from pathlib import PurePosixPath

class S3Path(object):
    def __init__(self, bucket, path, s3fs):
        self.s3fs = s3fs
        self.bucket = bucket
        self.path = PurePosixPath(path)
        if self.path.anchor != '/':
            self.path = '/' / self.path

    @property
    def bucket_path(self):
        return self.bucket + str(self.path)

    def exists(self):
        return self.s3fs.exists(self.bucket_path)

    def open(self, mode):
        if mode not in ['rb', 'wb']:
            raise ValueError('{mode} is not supported for s3fs paths'.format(**locals()))
        return self.s3fs.open(self.bucket_path, mode)

    @property
    def parent(self):
        return S3Path(self.bucket, self.path.parent, self.s3fs)

    def __truediv__(self, subdir):
        return S3Path(self.bucket, self.path / subdir, self.s3fs)

    def mkdir(self, parents=False, exist_ok=False):
        # s3 keystore does not require the 'parent directory' key to exist first
        # self.s3fs.mkdir(self.bucket_path)
        pass

    def remove(self):
        self.s3fs.rm(self.bucket_path)

    def __repr__(self):
        return 's3://{self.bucket}{self.path!s}'.format(**locals())

    def storage_options(self):
        if self.s3fs.key is not None and self.s3fs.secret is not None:
            return dict(key=self.s3fs.key, secret=self.s3fs.secret)
        else:
            raise RuntimeError('Now parameters given')
