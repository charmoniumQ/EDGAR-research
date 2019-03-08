from .config import config
from .provisioned_resource import FileProvisionedResource
from . import utils
from pathlib import Path
import subprocess
import itertools
import base64
import json
import time
import google
# https://googleapis.github.io/google-cloud-python/latest/container/gapic/v1/api.html
import  google.cloud.container_v1
# https://github.com/kubernetes-client/python/blob/master/kubernetes/README.md
import kubernetes
# import warnings
# warnings.simplefilter("always")


class GKECluster(FileProvisionedResource):
    def __init__(self, nodecount=1, cache_dir=None, name=None):
        self.nodecount = nodecount
        self.name = name
        self.name_path = f'projects/{config.gcloud.project}/locations/{config.gcloud.fq_zone}/clusters/{self.name}'
        self.cluster_manager = google.cloud.container_v1.ClusterManagerClient()
        self.provision_cluster()
        self.wait_for_gke()
        self.setup_kube_auth()
        super().__init__(nodecount, cache_dir=cache_dir, name=name)

    def __getstate__(self):
        # cluster_manager has a Channel and is not picklable
        return utils.omit(self.__dict__, set(['cluster_manager', 'kube_api']))

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.cluster_manager = google.cloud.container_v1.ClusterManagerClient()
        self.wait_for_gke()

        # setup_kube auth has to be redone because this it affects external state
        # merely unpickling the GKECluster will not restore the external state
        self.setup_kube_auth()

    @utils.time_code_decor()
    def provision_cluster(self):
        self.gke_cluster = google.cloud.container_v1.types.Cluster(
            name=self.name,
            initial_node_count=self.nodecount,
            node_config=google.cloud.container_v1.types.NodeConfig(
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
                service_account='main-722@edgar-research.iam.gserviceaccount.com',
            ),
            addons_config=google.cloud.container_v1.types.AddonsConfig(
                http_load_balancing=google.cloud.container_v1.types.HttpLoadBalancing(
                    disabled=True,
                ),
                horizontal_pod_autoscaling=google.cloud.container_v1.types.HorizontalPodAutoscaling(
                    disabled=True,
                ),
                kubernetes_dashboard=google.cloud.container_v1.types.KubernetesDashboard(
                    disabled=True,
                ),
                network_policy_config=google.cloud.container_v1.types.NetworkPolicyConfig(
                    disabled=True,
                ),
            ),
        )
        self.cluster_manager.create_cluster(None, None, self.gke_cluster, parent=str(Path(self.name_path).parent.parent))

    @utils.time_code_decor()
    def wait_for_gke(self):
        self.name_path = str(self.name_path)
        delays = itertools.chain([0, 5, 10], itertools.repeat(20))
        for _ in map(time.sleep, delays):
            self.gke_cluster = self.cluster_manager.get_cluster(None, None, None, name=self.name_path)
            if self.gke_cluster.status == google.cloud.container_v1.enums.Cluster.Status.RUNNING.value:
                break

    @utils.time_code_decor()
    def setup_kube_auth(self):
        subprocess.run([
            'gcloud', '--quiet', 'container', 'clusters',
            'get-credentials', self.gke_cluster.name,
            '--region', self.gke_cluster.location
        ], capture_output=True)
        kubernetes.config.load_kube_config()
        self.kube_api =  kubernetes.client.ApiClient()

    @utils.time_code_decor()
    def _delete(self):
        self.cluster_manager.delete_cluster(None, None, None, name=self.name_path)
        self.gke_cluster = None
        super()._delete()


if __name__ == '__main__':
    from pprint import pprint
    import logging
    logging.basicConfig(level=logging.INFO)
    g = GKECluster.create_or_load(
        nodecount=1,
        cache_dir=config.cache_dir,
        name='test-cluster-2',
        should_save=True
    )
    with g:
        pods = kubernetes.client.CoreV1Api(g.kube_api).list_namespaced_pod('default')
        pprint(pods)
