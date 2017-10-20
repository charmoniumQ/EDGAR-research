import string
import random
import boto3
import yaml
from edgar_code.util import find_file, BOX_PATH, rand_name
from .config import credentials, config, write_config


if __name__ == '__main__':
    # TODO: use random words instead
    config['bucket'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

    session = boto3.Session(
        aws_access_key_id=credentials['aws']['cache']['access_key_id'],
        aws_secret_access_key=credentials['aws']['cache']['secret_access_key'],
    )
    s3 = session.resource('s3')
    s3.create_bucket(
        Bucket=config['bucket'],
        CreateBucketConfiguration=dict(
            LocationConstraint='us-west-1',
        ),
    )

    write_config()
