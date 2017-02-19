#!/usr/lib/spark/bin/spark-submit
from itertools import islice
from pyspark import SparkContext, SparkConf
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors, ParseError
from glob import glob
import random
import string

i = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
name = 'EDGAR research ' + i
print(name)
# http://spark.apache.org/docs/latest/configuration.html
conf = (SparkConf()
        .setAppName(name)
        # .setMaster('spark://localhost:7077')
)
sc = SparkContext(conf=conf)
for egg in glob('dist/*.egg'):
    print('including', egg)
    sc.addPyFile(egg)

# https://spark.apache.org/docs/latest/programming-guide.html#rdd-operations
def mapi(index_info):
    path = index_info['Filename']
    try:
        risk_factors = get_risk_factors(path, debug=False, enable_cache=False)
    except ParseError as e:
        return (1, 0, 0)
    else:
        return (1, 1, len(risk_factors))

def reducei(i1, i2):
    return tuple(x1 + y1 for x1, y1 in zip(i1, i2))

form_index = sc.parallelize(list(islice(get_index(2016, 3, enable_cache=False), 0, 100)))

total, valid, size = form_index.map(mapi).reduce(reducei)
size = size / 1e6
avg_size = size / valid / 1000
hit_ratio = (valid / total)*100
print('{size:.1f} MB in total, {avg_size:.0f} KB per doc, {valid} valid / {total} = {hit_ratio:.0f}%')
