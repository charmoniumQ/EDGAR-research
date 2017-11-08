import copy
from .config import get_boto_session


def get_tags(instance):
    return {tag['Key']: tag['Value']
            for tag in instance.tags}



class EC2Provisioner(object):
    def __init__(self, boto3_kwargs, paramiko_kwargs):
        boto3 = get_boto_session('compute')
        self.ec2 = boto3.client('ec2')


    def find_instances(self, name, role_name):
        for instance in self.ec2.instances.all():
            if instance.state['Name'] == 'running'
            tags = get_tags(instance)
            if tags['cluster'] == name and tags['role_name'] == role_name:
                yield instance

    def create_instances(self, name, role_name, ec2_conf):
        ec2_conf = copy.deepcopy(ec2_conf)
        if 'TagSpecifications' not in ec2_conf:
            ec2_conf['TagSpecifications'] = []
        ec2_conf['TagSpecifications'].extend([
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'role',
                        'Value': role_name,
                    },
                    {
                        'Key': 'cluster',
                        'Value': name,
                    },
                ],
            },
        ])
        self.ec2.create_instances(**ec2_conf)

    def connect_to_instances(self, instances):
        pass

    def deprovision(instances):
        for instance in instances:
            instance.terminate()


class EC2Instance(object):
    pass
