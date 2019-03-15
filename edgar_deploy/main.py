from concurrent.futures import ThreadPoolExecutor
from .config import config
from .gke_cluster import GKECluster
from .kubernetes_deploy import kubernetes_namespace, setup_kubernetes
from .prepare_docker_images import prepare_docker_images
from .s3bucket import S3Bucket
from . import utils


images = prepare_docker_images(cache_dir=config.cache_dir)


cluster = GKECluster.create_or_load(
    nodecount=1,
    cache_dir=config.cache_dir,
    name=f'{config.name}-1',
    should_save=False,
)


s3bucket = S3Bucket.create_or_load(
    name=f'{cluster.name}-status',
    save=False,
    cache_dir=config.cache_dir,
)


with cluster:
    with s3bucket:
        namespace = f'{config.name}-{utils.rand_name(5, lowercase=True)}'
        with kubernetes_namespace(cluster.kube_api, namespace):
            setup_kubernetes(
                cluster.kube_api,
                namespace,
                cluster.nodecount,
                images,
                s3bucket.name,
            )
            print('done ish')
            input()
