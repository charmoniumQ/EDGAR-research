from edgar_code.cloud.ec2_provisioner import EC2Provisioner
from edgar_code.cloud.cluster import Cluster
from edgar_code.cloud.config import get_publickey
import IPython

slave_role = {
    'create_instances': {
        'ec2_conf': {
            # See: https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances

            # TODO: customize EBS
            # 'EbsOptimized': True,

            # To find the ImageId consult
            # https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Images:visibility=public-images;name=debian-stretch;sort=name
            'ImageId': 'ami-52c7df2b',
            'InstanceType': 't2.nano',
            'KeyName': 'edgar',
            'Placement': {
                'AvailabilityZone': 'us-west-2a',
            },
            'SecurityGroupIds': [
                # TODO: create security group
                'sg-116be16b',
            ],
        },
        'count': 1,
    },
    'setup': b"""#!/bin/sh
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-numpy
"""
}

cluster_conf = {
    'roles': {
        'slave': slave_role,
    },
    'provisioner': EC2Provisioner(**{
        'boto3_kwargs': {
            'aws_access_key_id': config['aws']['compute']['access_key_id'],
            'aws_secret_access_key': config['aws']['compute']['secret_access_key'],
            'region_name': config['aws']['region'],
        },
        'paramiko_kwargs': {
            # See: http://docs.paramiko.org/en/2.2/api/client.html#paramiko.client.SSHClient.connect
            'username': 'admin',
            'pkey': get_publickey(),
        }
    }),
}


if __name__ == '__main__':
    cluster = Cluster(**cluster_conf)
    with cluster.provisioned(name='solitary_flower', load=True, save=True):
        IPython.embed()
