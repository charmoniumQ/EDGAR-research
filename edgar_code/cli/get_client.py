import os
import dask.distributed

def get_client() -> dask.distributed.Client:
    # pylint: disable=eval-used
    dask_kwargs = eval(os.environ.get('DASK_DISTRIBUTED_KWARGS', '{}'))
    return dask.distributed.Client(**dask_kwargs)
