#!/usr/lib/spark/bin/spark-submit
from itertools import islice
from pyspark import SparkContext, SparkConf
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors, ParseError

# http://spark.apache.org/docs/latest/configuration.html
conf = (SparkConf()
        .setAppName('EDGAR research')
        .setMaster('local[*]')
)
sc = SparkContext(conf=conf)
sc.addPyFile('dist/EDGAR_research-0.1-py3.4.egg')


































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

form_index = sc.parallelize(list(get_index(2016, 3, enable_cache=False)))

total, valid, size = form_index.map(mapi).reduce(reducei)
print('total = {total}, valid = {valid}, avg size = {0}, percent worked = {1}').format(size / valid, valid / total, **locals())
