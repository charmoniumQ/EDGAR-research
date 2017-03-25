#!/usr/lib/spark/bin/spark-submit
import random
import string
import glob
import pyspark
import socket
import datetime
from haikunator import Haikunator
from itertools import islice
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_items, ParseError

name = 'EDGAR research ' + Haikunator.haikunate(delimiter=' ')
print(name)
conf = pyspark.SparkConf().setAppName(name)
sc = pyspark.SparkContext(conf=conf)
for egg in glob.glob('*.egg'):
    print('Adding egg ' + egg)
    sc.addPyFile(egg)

# https://spark.apache.org/docs/latest/programming-guide.html#rdd-operations
def download_risk(filing_info):
    path = filing_info['Filename']
    output = filing_info.copy()
    try:
        items = get_items(path, debug=False, enable_cache=False, verbose=False)
    except ParseError as e:
        output['error'] = True
    else:
        output['error'] = False
        output.update(items)
    return output

year = 2016
qtr = 3
index = get_index(year, qtr, enable_cache=False, verbose=False, debug=False)
start = datetime.datetime.now()
risk_data = sc.parallelize(islice(index, 20), 10).map(download_risk)
risk_data.saveAsPickleFile('risk_data-{year}-qtr{qtr}-part'.format(**locals()))
stop = datetime.datetime.now()
print('All risks downloaded {0:.0f}'.format((stop - start).total_seconds()))
