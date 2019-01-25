import edgar_code.cloud.config as config
from haikunator import Haikunator
import contextlib
import googleapiclient.discovery
import yaml

credentials, location = config.get_google_cred()
gke = googleapiclient.discovery.build('container', 'v1', credentials=credentials)
# API overview:
# https://developers.google.com/api-client-library/python/apis/container/v1
# API ref:
# https://developers.google.com/resources/api-libraries/documentation/container/v1/python/latest/container_v1.projects.locations.clusters.html

parent = f'projects/{credentials.project_id}/locations/{location}'

with open('./edgar_code/cloud/cluster_config.yaml') as fileobj:
    cluster_config = yaml.load(fileobj)
cluster_config['parent'] = parent
cluster_config['cluster']['locations'] = [location]
cluster_config['cluster']['initialNodeCount'] = 3
cluster_config['cluster']['nodeConfig']['serviceAccount'] = credentials.service_account_email
cluster_config['cluster']['name'] = 'test-cluster-1'
gke_clusters = gke.projects().locations().clusters()
gke_clusters.create(parent=parent, body=cluster_config).execute()

# HttpError: <HttpError 400 when requesting https://container.googleapis.com/v1/projects/edgar-reserach/locations/us-central1-a/clusters?alt=json returned
# "The user does not have access to service account "main-507@edgar-reserach.iam.gserviceaccount.com".
# Ask a project owner to grant you the iam.serviceAccountUser role on the service account.">

# class Cluster(object):
#     def __init__(self, provisioner, roles):
#         self.provisioner = provisioner
#         self.roles = roles

#     @contextlib.contextmanager
#     def provisioned(self, name=None, save=False, load=False):
#         if not name:
#             name = Haikunator.haikunate(0, '_')
#         self.name = name
#         self._provision_instances(load)
#         self._configure_instances()
#         yield
#         self._deprovision_instances(save)

#     def _provision_instances(self, load):
#         if load:
#             raise NotImplementedError()
#         else:
#             pass

#     def _configure_instances(self):
#         setup_script = b'./setup.sh'
#         command = b'nohup ./setup.sh > out.txt 2> err.txt &'
#         for role_name, role in self.roles.items():
#             for shell in role['shells']:
#                 sftp = shell.open_sftp()
#                 new_setup = io.BytesIO(role['setup'])
#                 sftp.putfo(new_setup, setup_script)
#                 sftp.chmod(setup_script, stat.S_IRWXU)
#                 shell.exec_command(command)

#     def _check_instances(self):
#         for role_name, role in self.roles.items():
#             for shell in role['shells']:
#                 sftp = shell.open_sftp()
#                 pass

#     def _deprovision_instances(self, save):
#         if not save:
#             for role in self.roles:
#                 self.provisioner.deprovision(role['instances'])
