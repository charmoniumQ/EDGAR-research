import io
import urllib.parse
import boto3
import botocore


s3 = boto3.resource('s3')


class S3Path(io.BytesIO):
    @classmethod
    def parse_s3url(cls, url):
        url = urllib.parse.urlparse(url)
        if url.scheme != 's3':
            raise ValueError('Must be an s3 scheme')
        bucket = url.netloc
        key = url.path[1:]
        return bucket, key

    def __init__(self, url):
        super().__init__()
        self.bucket, self.key = S3Path.parse_s3url(url)
        self.object_ = s3.Object(self.bucket, self.key)
        # self.stream = io.BytesIO()
        if self.exists():
            self.object_.download_fileobj(self)
            print(2, self.read())
            self.seek(0)

    # def write(self, bytestr):
    #     self.stream.write(bytestr)

    # def read(self, count=-1):
    #     return self.stream.read(count)

    # def readline(self):
    #     return self.stream.readline()

    def close(self):
        self.seek(0)
        print(1, self.read())
        self.seek(0)
        self.object_.upload_fileobj(self)

    def exists(self):
        try:
            self.object_.get()
        except botocore.exceptions.ClientError:
            return False
        else:
            return True


if __name__ == '__main__':
    a = S3Path('s3://jlndhfkiab-edgar-data/test8')
    a.write(b'hello')
    a.flush()
