#!/usr/bin/env python3
import os
import sys
import dask.distributed
import dask.bag
import socket
import traceback

scheduler_address = os.environ['dask_scheduler_address']

client = dask.distributed.Client(scheduler_address)

bag = dask.bag.from_sequence(list(range(10)))
def mapper(i):
    return socket.gethostname()
print(set(bag.map(mapper)))
