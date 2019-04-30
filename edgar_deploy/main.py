from concurrent.futures import ThreadPoolExecutor
from .config import config
from .gke_cluster import GKECluster
from .kubernetes_deploy import kubernetes_namespace, setup_kubernetes
from .prepare_docker_images import prepare_docker_images
from .prepare_egg import prepare_egg
from .s3bucket import S3Bucket
from . import utils


# cores_per_node = 64
# node_count = 1
# worker_count = cores_per_node * node_count - 5
# machine_type = f'n1-highmem-{cores_per_node}'

# cores_per_node = 8
# node_count = 8
# worker_count = node_count
# machine_type = f'n1-highmem-{cores_per_node}'

# cores_per_node = 40
# node_count = 1
# worker_count = 12
# machine_type = f'n1-ultramem-40'

cores_per_node = 2
node_count = 12
worker_count = 12
disk_size = int(8 + 1.5 * (worker_count / node_count))
machine_type = f'n1-highmem-{cores_per_node}'
run_module = 'edgar_code.executables.tokenize_rfs'
# run_module = 'edgar_code.executables.search_rfs'

cluster_name = f'{config.name}-1'
namespace = f'{config.name}-{utils.rand_name(5, lowercase=True)}'
egg_name = f'eggs/edgar_code-{namespace}.egg'


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
        disk_size=disk_size,
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
                run_module,
            )
            print('done ish')
            input()
