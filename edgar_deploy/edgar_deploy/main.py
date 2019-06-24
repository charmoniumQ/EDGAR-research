import logging
from concurrent.futures import ThreadPoolExecutor
from .config import config
from .gke_cluster import GKECluster
from .kubernetes_deploy import kubernetes_namespace, setup_kubernetes
from .prepare_egg import prepare_egg
from .gs_path import GSPath
from . import utils
from .prepare_docker_images import prepare_docker_images

logging.getLogger('edgar_deploy.time_code').setLevel('DEBUG')

# cores_per_node = 8
# node_count = 8
# worker_count = node_count
# machine_type = f'n1-highmem-{cores_per_node}'

cores_per_node = 1
node_count = 4
worker_count = node_count * cores_per_node - 2
disk_size = int(10 + 4 * (worker_count / node_count))
machine_type = f'n1-standard-{cores_per_node}'
run_module = 'edgar_code.cli.get_rfs'


cluster_name = f'{config.name}-1'
namespace = f'{config.name}-{utils.rand_name(5, lowercase=True)}'
egg_path = GSPath.from_url(f'gs://results-7/eggs/edgar_code-{namespace}.egg')
images = prepare_docker_images(cache_dir=config.cache_dir)
public_egg_path = prepare_egg(egg_path)
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
            public_egg_path,
            run_module,
        )
        print('done ish')
        input()
