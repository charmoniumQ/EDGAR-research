import sys
import dask.distributed
import dask.bag
import socket
import traceback

scheduler_address = sys.argv[1]

for i in range(20):
    print(f'attempt {i}')
    try:
        client = dask.distributed.Client(scheduler_address)
    except:
        traceback.print_exc()
    else:
        break

bag = dask.bag.from_sequence(list(range(10)))
def mapper(i):
    return f'{i} on {socket.gethostname()}'
result = list(bag.map(mapper))
print(result)
