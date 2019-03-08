from concurrent.futures import ThreadPoolExecutor
from .config import config
from .gke_cluster import GKECluster
from .kubernetes_deploy import kubernetes_namespace, setup_kubernetes
from .prepare_docker_images import prepare_docker_images

prepare_docker_images()


with ThreadPoolExecutor(max_workers=3) as executor:
    cluster = GKECluster.create_or_load(
        nodecount=1,
        cache_dir=config.cache_dir,
        name='test-cluster-2',
        should_save=True
    )

    with cluster:
        images = prepare_docker_images()
        #bucket = create_storage()
        with kubernetes_namespace(cluster.kube_api, 'eddy'):
            setup_kubernetes(cluster.kube_api, 'eddy', cluster.nodecount, images)
            print('done ish')
            input()
