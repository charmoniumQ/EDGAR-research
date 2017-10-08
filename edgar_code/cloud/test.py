from code.cloud.ec2_provisioner import EC2Provisioner
from code.cloud.cluster import Cluster
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
    'connect_to_instances': {
        'paramiko_conf': {
            # See: http://docs.paramiko.org/en/2.2/api/client.html#paramiko.client.SSHClient.connect
            'username': 'admin',
            'key_filename': 'edgar.pem',
        }
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
    'provisioner': EC2Provisioner({
        # See: https://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session.resource
        'aws_access_key_id': 'AKIAJK2TQPGVPBQLSVRA',
        'aws_secret_access_key': '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',
        'region_name': 'us-west-2',
    }),
}


def main():
    cluster = Cluster(**cluster_conf)
    with cluster.provisioned(name='solitary_flower', load=True, save=True):
        IPython.embed()

if __name__ == '__main__':
    main()
