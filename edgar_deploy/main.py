from concurrent.futures import ThreadPoolExecutor
from .gke_cluster import GKECluster
from .kubernetes_deploy import prepare_images, kubernetes_namespace, setup_kubernetes

#prepare_images = lambda: None

with ThreadPoolExecutor(max_workers=3) as executor:
    cluster = GKECluster('test-cluster-2', load=True, save=True, nodecount=3)

    images_fut = executor.submit(prepare_images)
    cluster_fut = executor.submit(cluster.open)

    images = images_fut.result()
    cluster_fut.result()

    with cluster:
        with kubernetes_namespace(cluster.kube_api, cluster.managed_namespace):
            setup_kubernetes(cluster.kube_api, cluster.managed_namespace, cluster.nodecount)
            print('done ish')
            input()
