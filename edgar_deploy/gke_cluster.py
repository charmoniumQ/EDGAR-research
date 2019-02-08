from .config import config
from .cluster import Cluster
import google
from google.cloud import container_v1
import kubernetes
import subprocess
import base64
import json
import time


class GKECluster(Cluster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster_manager = container_v1.ClusterManagerClient(credentials=config.gcloud.credentials)
        self.username = 'main-user'
        self.namespace = 'dask'
        self.gke_cluster = None
        self.parent_path = f"projects/{config.gcloud.project}/locations/{config.gcloud.fq_zone}"
        self.name_path = f"projects/{config.gcloud.project}/locations/{config.gcloud.fq_zone}/clusters/{self.name}"

    def _create(self):
        self.gke_cluster = container_v1.types.Cluster(
            name=self.name,
            initial_node_count=self.nodecount,
            node_config=container_v1.types.NodeConfig(
                # Consider changing this for cost-effectiveness
                machine_type='n1-standard-1',
                # in GB, minimum is 10
                disk_size_gb=10,
                # TODO: examine the effect of this
                preemptible=True,
            ),
            addons_config=container_v1.types.AddonsConfig(
                http_load_balancing=container_v1.types.HttpLoadBalancing(
                    disabled=True,
                ),
                horizontal_pod_autoscaling=container_v1.types.HorizontalPodAutoscaling(
                    disabled=True,
                ),
                kubernetes_dashboard=container_v1.types.KubernetesDashboard(
                    disabled=True,
                ),
                network_policy_config=container_v1.types.NetworkPolicyConfig(
                    disabled=True,
                ),
            ),
        )
        #self.gke_cluster = self.cluster_manager.create_cluster(None, None, self.gke_cluster, parent=self.parent_path)
        self.gke_cluster = self.cluster_manager.get_cluster(None, None, None, name=self.name_path)

        # tried: https://banzaicloud.com/blog/pipeline-gke-rbac/
        # tried: https://stackoverflow.com/a/48377444/1078199
        # using: https://stackoverflow.com/questions/54410410/authenticating-to-gke-master-in-python/54534575#54534575
        subprocess.run(['gcloud', '--quiet', 'container', 'clusters',
                        'get-credentials', self.gke_cluster.name,
                        '--region', self.gke_cluster.location],
                       capture_output=True)

        subprocess.run(['kubectl', 'create', '-f', '-'], input=json.dumps({
                'apiVersion': 'v1',
                'kind': 'Namespace',
                'metadata': {
                    'name': self.namespace,
                },
            }).encode())

        subprocess.run(['kubectl', 'create', '-f', '-'], input=json.dumps({
                'apiVersion': 'v1',
                'kind': 'ServiceAccount',
                'metadata': {
                    'name': self.username,
                    'namespace': self.namespace,
                },
            }).encode())

        subprocess.run(['kubectl', 'create', '-f', '-'], input=json.dumps({
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

        self._configure_access()

    def _load(self):
        try:
            self.gke_cluster = self.cluster_manager.get_cluster(None, None, None, name=self.name_path)
        except google.api_core.exceptions.NotFound:
            return False
        else:
            self._configure_access()
            return True

    def _delete(self):
        self.cluster_manager.delete_cluster(None, None, None, name=self.name_path)
        self.gke_cluster = None

    def _configure_access(self):
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
            logger.setLevel(config.logging_level)

        self.kube_config.host = f'https://{self.gke_cluster.endpoint}'
        self.kube_config.ssl_ca_cert = config.scratch_dir / 'ssl_ca_cert'
        with open(self.kube_config.ssl_ca_cert, 'wb') as f:
            f.write(base64.decodebytes(
                self.gke_cluster.master_auth.cluster_ca_certificate.encode()
            ))

        # self.kube_config.cert_file = dire / 'cert_file'
        # with self.kube_config.cert_file.open('wb') as f:
        #     f.write(base64.decodebytes(client_cert))

        self.kube_api = kubernetes.client.ApiClient(configuration=self.kube_config)
        self.kube_v1 = kubernetes.client.CoreV1Api(self.kube_api)


if __name__ == '__main__':
    from pprint import pprint
    import logging
    logging.basicConfig(level=logging.INFO)
    g = GKECluster('test-cluster-1', load=True, save=True)
    with g:
        pods = g.kube_v1.list_namespaced_pod(g.namespace)
        pprint(pods)
