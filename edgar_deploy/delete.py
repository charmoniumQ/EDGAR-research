from .config import config
from .gke_cluster import GKECluster

cluster = GKECluster.create_or_load(
    nodecount=8,
    cache_dir=config.cache_dir,
    name=f'{config.name}-1',
    should_save=False,
)


with cluster:
    pass
