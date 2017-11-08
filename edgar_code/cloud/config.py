from s3fs import S3FileSystem
import io
import yaml
import boto3
import paramiko
from edgar_code.util import find_file, BOX_PATH, rand_name
from .s3path import S3Path


# TODO: completely encapsulate config.yaml so that nobody else gets to see/care
# about its schema.


config_file = find_file('config.yaml', BOX_PATH)
if not config_file:
    raise RuntimeError('No config.yaml found in BOX_PATH: {}'.format(BOX_PATH))
else:
    with config_file.open() as f:
        config = yaml.load(f)
    if 'bucket' not in config['aws']['cache']:
        setup_s3()
    # if 'keypair' not in config['aws']['compute']:
    #     setup_keypair()


def setup_s3():
    config['aws']['cache']['bucket'] = rand_name()
    session = get_boto_session('cache')
    s3 = session.resource('s3')
    s3.create_bucket(
        Bucket=config['aws']['cache']['bucket'],
    )
    write_config()    


def setup_keypair():
    config['aws']['compute']['keypair'] = {}
    boto3 = get_boto_session('compute')
    ec2 = boto3.resource('ec2')
    keypair = ec2.create_key_pair(**{
        'KeyName': 'edgar_key',
    })
    config['aws']['compute']['keypair']['private'] = keypair.key_material
    write_config()


def get_publickey():
    pkey = config['aws']['compute']['keypair']['private']
    return paramiko.RSAKey.from_private_key(StringIO(pkey))


def write_config():
    with config_file.open('w+') as f:
        yaml.dump(config, f)


def get_s3fs(purpose):
    # different purposes have different IAM users with different access levels
    # it also makes more granular logging if you have logging by IAM user
    return S3FileSystem(
        key=config['aws'][purpose]['access_key_id'],
        secret=config['aws'][purpose]['secret_access_key']
    )


def get_boto_session(purpose):
    return boto3.Session(
        aws_access_key_id=config['aws'][purpose]['access_key_id'],
        aws_secret_access_key=config['aws'][purpose]['secret_access_key'],
        region_name=config['aws']['region']
    )


def get_s3path(purpose, path):
    # different purposes may also have different buckets
    return S3Path(bucket=config['aws'][purpose]['bucket'],
                  path=path,
                  s3fs=get_s3fs(purpose),
    )
