# https://github.com/googleapis/google-api-python-client/issues/325
import edgar_code.cloud.config as config

discovery_cache_dir = config.get_cachedir() / 'google_api_discovery'
discovery_cache_dir.mkdir(parents=True, exist_ok=True)
class DiscoveryCache(object):
    def filename(self, url):
        return str(discovery_cache_dir / url.replace('/', '_'))

    def get(self, url):
        try: 
           with open(self.filename(url), 'r') as f:
                return f.read()
        except FileNotFoundError:
            return None

    def set(self, url, content):
        with open(self.filename(url), 'w+') as f:
            f.write(content)
            f.flush()

from edgar_code.cloud.cluster import Cluster
import kubernetes.client
import googleapiclient.discovery
import yaml
import time
import tempfile
import subprocess
import base64
from pathlib import Path

# TODO: re-examine the role of module-level variables vs config variables
credentials, location = config.get_google_cred()
gke = googleapiclient.discovery.build('container', 'v1', credentials=credentials, cache=DiscoveryCache())
parent = f'projects/{credentials.project_id}/locations/{location}'
gke_clusters = gke.projects().locations().clusters()

# API overview:
# https://developers.google.com/api-client-library/python/apis/container/v1
# API ref:
# https://developers.google.com/resources/api-libraries/documentation/container/v1/python/latest/container_v1.projects.locations.clusters.html
# TODO: switch to https://googleapis.github.io/google-cloud-python/latest/container/index.html
# https://googleapis.github.io/google-cloud-python/latest/container/gapic/v1/api.html

class GKECluster(Cluster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.username = 'main-user'
        self.namespace = 'dask'

    def _cluster_create(self):
        with open('./edgar_code/cloud/cluster_config.yaml') as fileobj:
            cluster_config = yaml.load(fileobj)
        cluster_config['parent'] = parent
        cluster_config['cluster']['locations'] = [location]
        cluster_config['cluster']['initialNodeCount'] = self.nodecount
        cluster_config['cluster']['nodeConfig']['serviceAccount'] = credentials.service_account_email
        cluster_config['cluster']['name'] = self.name
        self.gke_cluster = gke_clusters.create(parent=parent, body=cluster_config).execute()
        self.name_path = f'{parent}/clusters/{self.name}'
        print(self.gke_cluster)
        self._cluster_initialize()
        self._cluster_configure_access()

    def _cluster_load(self):
        self.name_path = f'{parent}/clusters/{self.name}'
        try:
            self.gke_cluster = gke_clusters.get(name=self.name_path).execute()
        except googleapiclient.errors.HttpError:
            return False
        else:
            self._cluster_configure_access()
            return True

    def _cluster_initialize(self):
        # tried: https://banzaicloud.com/blog/pipeline-gke-rbac/
        # tried: https://stackoverflow.com/a/48377444/1078199
        # using: https://stackoverflow.com/questions/54410410/authenticating-to-gke-master-in-python/54534575#54534575
        subprocess.run(['gcloud', '--quiet', 'container', 'clusters',
                        'get-credentials', self.gke_cluster["name"],
                        '--region', self.gke_cluster["location"]],
                       capture_output=True)

        subprocess.run(['kubectl', 'create', '-f', '-'], input=yaml.dump({
                'apiVersion': 'v1',
                'kind': 'Namespace',
                'metadata': {
                    'name': self.namespace,
                },
            }).encode())

        subprocess.run(['kubectl', 'create', '-f', '-'], input=yaml.dump({
                'apiVersion': 'v1',
                'kind': 'ServiceAccount',
                'metadata': {
                    'name': self.username,
                    'namespace': self.namespace,
                },
            }).encode())

        subprocess.run(['kubectl', 'create', '-f', '-'], input=yaml.dump({
                'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': 'RoleBinding',
                'metadata': {
                    'name': f'{self.username}-rolebinding',
                    'namespace': self.namespace,
                },
                'subjects': [
                    {
                        'kind': 'ServiceAccount',
                        'name': self.username,
                        'namespace': self.namespace,
                    },
                ],
                'roleRef': {
                    'apiGroup': 'rbac.authorization.k8s.io',
                    'kind': 'ClusterRole',
                    'name': 'cluster-admin',
                },
            }).encode())

    def _cluster_configure_access(self):
        secret_name = subprocess.run(['kubectl',  '--namespace', self.namespace,
                                      'get', f'serviceaccounts/{self.username}',
                                      '-o', 'jsonpath={.secrets[0].name}'],
                                     capture_output=True, encoding='ascii').stdout
        if not secret_name:
            raise RuntimeError(f'no secret corresponding to serviceaccounts/{self.username}')

        client_cert = subprocess.run(['kubectl',  '--namespace', self.namespace,
                                      'get', f'secret/{secret_name}',
                                      '-o', 'jsonpath={.data.ca\.crt}'],
                                     capture_output=True).stdout
        if not client_cert:
            raise RuntimeError(f'no client cert in secret/{secret_name}')

        client_token = subprocess.run(['kubectl',  '--namespace', self.namespace,
                                       'get', f'secret/{secret_name}',
                                       '-o', 'jsonpath={.data.token}'],
                                      capture_output=True).stdout
        if not client_cert:
            raise RuntimeError(f'no client token in secret/{secret_name}')

        self.kube_config = kubernetes.client.Configuration()
        self.kube_config.verify_ssl = True
        self.kube_config.api_key['authorization'] = base64.decodebytes(client_token).decode()
        self.kube_config.api_key_prefix['authorization'] = 'Bearer'

        for logger in self.kube_config.logger.values():
            logger.setLevel(logging.INFO)

        while 'endpoint' not in self.gke_cluster:
            logging.info('Wait while cluster is provisioning')
            self.gke_cluster = gke_clusters.get(name=self.name_path).execute()
            time.sleep(10)

        self.kube_config.host = f'https://{self.gke_cluster["endpoint"]}'
        self.kube_config.ssl_ca_cert = config.get_tempdir() / 'ssl_ca_cert'
        with open(self.kube_config.ssl_ca_cert, 'wb') as f:
            f.write(base64.decodebytes(self.gke_cluster['masterAuth']['clusterCaCertificate'].encode()))

        # self.kube_config.cert_file = dire / 'cert_file'
        # with self.kube_config.cert_file.open('wb') as f:
        #     f.write(base64.decodebytes(client_cert))

        self.kube_api = kubernetes.client.ApiClient(configuration=self.kube_config)
        self.kube_v1 = kubernetes.client.CoreV1Api(self.kube_api)

    def _cluster_delete(self):
        gke_clusters.delete(name=self.name_path).execute()

if __name__ == '__main__':
    from pprint import pprint
    import logging
    logging.basicConfig(level=logging.INFO)
    g = GKECluster('test-cluster-1', load=True, save=False)
    with g:
        pods = g.kube_v1.list_namespaced_pod(g.namespace)
        pprint(pods)
