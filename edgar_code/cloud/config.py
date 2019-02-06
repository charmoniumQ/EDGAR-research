from s3fs import S3FileSystem
import io
import yaml
import boto3
import docker
import tempfile
from edgar_code.util import find_file, BOX_PATH, rand_name
from google.oauth2 import service_account
from pathlib import Path
from .s3path import S3Path


# TODO: completely encapsulate config.yaml so that nobody else gets to see/care
# about its schema.

# TODO: this should not be in `cloud` subpackage

# TODO: this should not be self-modifying. The self-modifying part
# should be in a different file. It is conceptually a 'store' or
# 'cache' not a 'config'.

# TODO: use a singleton object with properties

config_file = find_file('config.yaml', BOX_PATH)
if not config_file:
    raise RuntimeError('No config.yaml found in BOX_PATH: {}'.format(BOX_PATH))
else:
    with config_file.open() as f:
        config = yaml.load(f)
    if 'bucket' not in config['aws']['cache']:
        setup_s3()


def setup_s3():
    config['aws']['cache']['bucket'] = rand_name()
    session = _get_boto_session('cache')
    s3 = session.resource('s3')
    s3.create_bucket(
        Bucket=config['aws']['cache']['bucket'],
    )
    write_config()    


def write_config():
    with config_file.open('w+') as f:
        yaml.dump(config, f)


def _get_s3fs(purpose):
    # different purposes have different IAM users with different access levels
    # it also makes more granular logging if you have logging by IAM user
    return S3FileSystem(
        key=config['aws'][purpose]['access_key_id'],
        secret=config['aws'][purpose]['secret_access_key']
    )


def _get_boto_session(purpose):
    return boto3.Session(
        aws_access_key_id=config['aws'][purpose]['access_key_id'],
        aws_secret_access_key=config['aws'][purpose]['secret_access_key'],
        region_name=config['aws']['region']
    )


def get_s3path(purpose, path):
    # different purposes may also have different buckets
    return S3Path(bucket=config['aws'][purpose]['bucket'],
                  path=path,
                  s3fs=_get_s3fs(purpose),
    )

def get_docker():
    client = docker.from_env()
    client.login(**config['docker']['auth'])
    repo = config['docker']['repository']
    name = config['docker']['name']
    return client, repo, name

def get_google_cred():
    cred = service_account.Credentials.from_service_account_info(
        info=config['gcloud']['service_account']
    )
    return cred, config['gcloud']['location']

def get_google_oauth():
    cred = service_account.Credentials.from_service_account_info(
        info=config['gcloud']['service_account']
    )
    return cred, config['gcloud']['location']

_tempdir = tempfile.TemporaryDirectory()
def get_tempdir():
    return Path(_tempdir.name)

_cachedir = Path(tempfile.gettempdir()) / 'edgar_code'
def get_cachedir():
    return _cachedir
