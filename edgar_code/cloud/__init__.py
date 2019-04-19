from .kv_bag import KVBag
from .storage import GSPath, copy
from .bag_store import BagStore


# TODO: move bag path to cache path
cache_path = GSPath.from_url('gs://results-7')
results_path = GSPath.from_url('gs://results-7/results')
