from .config import config
from .cluster import Cluster
from . import utils
import subprocess
import base64
import json
import time
import google
# https://googleapis.github.io/google-cloud-python/latest/container/gapic/v1/api.html
from google.cloud import container_v1
# https://github.com/kubernetes-client/python
import kubernetes
# import warnings
# warnings.simplefilter("always")


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
                # for GCR https://googleapis.github.io/google-cloud-python/latest/container/gapic/v1/types.html#google.cloud.container_v1.types.NodeConfig.oauth_scopes
                oauth_scopes=[
                    'https://www.googleapis.com/auth/devstorage.read_only',
                ],
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
        with utils.time_code('gke cluster create'):
            self.cluster_manager.create_cluster(None, None, self.gke_cluster, parent=self.parent_path)

        with utils.time_code('gke cluster provision'):
            while True:
                self.gke_cluster = self.cluster_manager.get_cluster(None, None, None, name=self.name_path)
                if self.gke_cluster.status == google.cloud.container_v1.enums.Cluster.Status.RUNNING.value:
                    break
                time.sleep(10)

        # tried: https://banzaicloud.com/blog/pipeline-gke-rbac/
        # tried: https://stackoverflow.com/a/48377444/1078199
        # using: https://stackoverflow.com/questions/54410410/authenticating-to-gke-master-in-python/54534575#54534575
        with utils.time_code('gcloud auth'):
            subprocess.run([
                'gcloud', '--quiet', 'container', 'clusters',
                'get-credentials', self.gke_cluster.name,
                '--region', self.gke_cluster.location
            ], capture_output=True)

        with utils.time_code('kube setup'):
            kubernetes.config.load_kube_config()
            kube_api = kubernetes.client.ApiClient()
            kube_v1 = kubernetes.client.CoreV1Api(kube_api)
            kube_v1.create_namespace(
                kubernetes.client.V1Namespace(
                    metadata=kubernetes.client.V1ObjectMeta(
                        name=self.namespace,
                    ),
                ),
            )
            kube_v1.create_namespaced_service_account(
                self.namespace,
                kubernetes.client.V1ServiceAccount(
                    metadata=kubernetes.client.V1ObjectMeta(
                        name=self.username,
                    ),
                ),
            )

            kube_rbac_authorization_v1 = kubernetes.client.RbacAuthorizationV1Api(kube_api)
            kube_rbac_authorization_v1.create_namespaced_role_binding(
                self.namespace,
                kubernetes.client.V1RoleBinding(
                    metadata=kubernetes.client.V1ObjectMeta(
                        name=f'{self.username}-rolebinding',
                    ),
                    role_ref=kubernetes.client.V1RoleRef(
                        api_group='rbac.authorization.k8s.io',
                        kind='ClusterRole',
                        name='cluster-admin',
                    ),
                    subjects=[
                        kubernetes.client.V1Subject(
                            kind='ServiceAccount',
                            name=self.username,
                            namespace=self.namespace,
                        ),
                    ],
                ),
            )
            service_account = kube_v1.read_namespaced_service_account(self.username, self.namespace)
            secret = kube_v1.read_namespaced_secret(service_account.secrets[0].name, self.namespace)

        self._configure_access(secret)

    def _load(self):
        try:
            with utils.time_code('gke cluster list'):
                self.gke_cluster = self.cluster_manager.get_cluster(None, None, None, name=self.name_path)
        except google.api_core.exceptions.NotFound:
            return False
        else:
            with utils.time_code('gcloud auth'):
                subprocess.run([
                    'gcloud', '--quiet', 'container', 'clusters',
                    'get-credentials', self.gke_cluster.name,
                    '--region', self.gke_cluster.location
                ], capture_output=True)
            with utils.time_code('kube load setup'):
                kubernetes.config.load_kube_config()
                kube_api = kubernetes.client.ApiClient()
                kube_v1 = kubernetes.client.CoreV1Api(kube_api)
                service_account = kube_v1.read_namespaced_service_account(self.username, self.namespace)
                secret = kube_v1.read_namespaced_secret(service_account.secrets[0].name, self.namespace)
            self._configure_access(secret)
            return True

    def _delete(self):
        self.cluster_manager.delete_cluster(None, None, None, name=self.name_path)
        self.gke_cluster = None

    def _configure_access(self, secret):
        token = base64.decodebytes(secret.data['token'].encode())
        self.kube_config = kubernetes.client.Configuration()
        self.kube_config.verify_ssl = True
        self.kube_config.api_key['authorization'] = token
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
        #     f.write(client_cert)

        self.kube_api = kubernetes.client.ApiClient(configuration=self.kube_config)
        self.kube_v1 = kubernetes.client.CoreV1Api(self.kube_api)


if __name__ == '__main__':
    from pprint import pprint
    import logging
    logging.basicConfig(level=logging.INFO)
    g = GKECluster('test-cluster-1', load=True, save=False)
    with g:
        pods = g.kube_v1.list_namespaced_pod(g.namespace)
        pprint(pods)
