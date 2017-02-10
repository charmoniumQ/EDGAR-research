#!/usr/lib/spark/bin/spark-submit
from pyspark import SparkContext, SparkConf
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors, ParseError
from glob import glob

# http://spark.apache.org/docs/latest/configuration.html
conf = SparkConf().setAppName('EDGAR researcn').setMaster('local[*]')
for pyfile in glob('*.py'):
    conf.addPyFile(pyfile)
sc = SparkContext(conf=conf)

# https://spark.apache.org/docs/latest/programming-guide.html#rdd-operations
def process_index(index_info):
    path = index_info['Filename']
    try:
        risk_factors = get_risk_factors(path, debug=False)
    except ParseError as e:
        return (index_info['CIK'], (str(e), index_info['Date Filed']))
    else:
        return (index_info['CIK'], (risk_factors, index_info['Date Filed']))

form_index = sc.parallelize(list(get_index(2016, 3)))
form_index.map(process_index)
