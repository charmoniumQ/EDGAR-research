from s3fs import S3FileSystem
import yaml
from edgar_code.util import find_file, BOX_PATH
import toolz
from .s3path import S3Path


# TODO: completely encapsulate config.yaml so that nobody else gets to see/care
# about its schema.


config_file = find_file('config.yaml', BOX_PATH)
if not config_file:
    raise RuntimeError('No config.yaml found in BOX_PATH: {}'.format(BOX_PATH))
else:
    with config_file.open() as f:
        config = yaml.load(f)


def write_config():
    with config_file.open('w+') as f:
        yaml.dump(config, f)


def get_s3fs(purpose):
    # different purposes have different IAM users with different access levels
    return S3FileSystem(
        key=config['aws'][purpose]['access_key_id'],
        secret=config['aws'][purpose]['secret_access_key']
    )


def get_boto_session(purpose):
    return boto3.Session(
        aws_access_key_id=config['aws'][purpose]['access_key_id'],
        aws_secret_access_key=config['aws'][purpose]['secret_access_key'],
    )


def get_s3path(purpose, path):
    # different purposes may also have different buckets
    return S3Path(bucket=config['aws'][purpose]['bucket'],
                  path=path,
                  s3fs=get_s3fs(purpose),
    )
