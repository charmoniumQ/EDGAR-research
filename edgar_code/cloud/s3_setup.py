import string
import random
import boto3
import yaml
from edgar_code.util import find_file, BOX_PATH, rand_name
from .config import config, write_config, get_boto_session


if __name__ == '__main__':
    # TODO: make this script run if necessary without intervention
    # perhaps be part of config.py

    # TODO: use random words instead
    config['aws']['cache']['bucket'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

    session = get_boto_session('cache')

    s3 = session.resource('s3')
    s3.create_bucket(
        Bucket=config['aws']['cache']['bucket'],
        CreateBucketConfiguration=dict(
            LocationConstraint=config['aws']['cache']['region'],
        ),
    )

    write_config()
