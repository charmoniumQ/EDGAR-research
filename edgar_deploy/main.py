from concurrent.futures import ThreadPoolExecutor
from .config import config
from .gke_cluster import GKECluster
from .kubernetes_deploy import kubernetes_namespace, setup_kubernetes
from .prepare_docker_images import prepare_docker_images
from . import utils


images = prepare_docker_images(cache_dir=config.cache_dir)


cluster = GKECluster.create_or_load(
    nodecount=1,
    cache_dir=config.cache_dir,
    name='test-cluster-2',
    should_save=True
)


with cluster:
    namespace = 'ed-' + utils.rand_name(5, lowercase=True)
    with kubernetes_namespace(cluster.kube_api, namespace):
        setup_kubernetes(cluster.kube_api, namespace,
                         cluster.nodecount, images)
        print('done ish')
        input()
