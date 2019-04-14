from concurrent.futures import ThreadPoolExecutor
from .config import config
from .gke_cluster import GKECluster
from .kubernetes_deploy import kubernetes_namespace, setup_kubernetes
from .prepare_docker_images import prepare_docker_images
from .prepare_egg import prepare_egg
from .s3bucket import S3Bucket
from . import utils


cores_per_node = 2
node_count = 8
cluster_name = f'{config.name}-2'
worker_count = cores_per_node * node_count
namespace = f'{config.name}-{utils.rand_name(5, lowercase=True)}'
egg_name = f'eggs/edgar_code-{namespace}.egg'
machine_type = f'n1-highmem-{cores_per_node}'


images = prepare_docker_images(cache_dir=config.cache_dir)


s3bucket = S3Bucket.create_or_load(
    name=f'{cluster_name}-status',
    save=False,
    cache_dir=config.cache_dir,
)


with s3bucket:
    prepare_egg(s3bucket.bucket.blob(egg_name))

    cluster = GKECluster.create_or_load(
        nodecount=node_count,
        machine_type=machine_type,
        cache_dir=config.cache_dir,
        name=cluster_name,
        should_save=True,
    )

    with cluster:
        with kubernetes_namespace(cluster.kube_api, namespace):
            setup_kubernetes(
                cluster.kube_api,
                namespace,
                worker_count,
                images,
                s3bucket.name,
            )
            print('done ish')
            input()
