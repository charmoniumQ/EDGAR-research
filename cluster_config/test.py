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
from mining.retrieve_10k import get_risk_factors, ParseError

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
    start = datetime.datetime.now()
    try:
        risks = get_risk_factors(path, debug=False, enable_cache=False, verbose=False)
    except ParseError as e:
        output['risk'] = None
    else:
        output['risk'] = risks
    stop = datetime.datetime.now()
    output['time'] = (stop - start).total_seconds()
    output['downloaded_by'] = socket.gethostname()
    return output

start = datetime.datetime.now()
index = get_index(2016, 3, enable_cache=False, verbose=False, debug=False)
risk_data = sc.parallelize(islice(index, 50), 10).map(download_risk)
risk_data.persist()
stop = datetime.datetime.now()
print('All risks downloaded {0:.0f}'.format((stop - start).total_seconds()))
# risk_data.saveAsSequenceFile('risk_data')

def count(filing):
    if filing['risk'] is None:
        return dict(total=1, valid=0, size=0)
    else:
        return dict(total=1, valid=1, size=len(filing['risk']))

def reduce(count1, count2):
    return {key: count1[key] + count2[key] for key in ['total', 'valid', 'size']}

result = risk_data.map(count).reduce(reduce)
total, valid, size = result['total'], result['valid'], result['size']
size = size / 1e6
print(3/2)
avg_size = (size / valid)
hit_ratio = (valid / total) * 1e2
stop = datetime.datetime.now()
print('{size:.1f} MB in total, {avg_size:f} B per doc, {valid} / {total} valid = {hit_ratio:.0f}%'.format(**locals()))

def key_by_downloader(filing):
    return (filing['downloaded_by'], 1)

def add(a, b):
    return a + b

print(risk_data.map(key_by_downloader).reduceByKey(add).collectAsMap())

def get_time(filing):
    return filing['time']

print(sorted(risk_data.map(get_time).collect())[-50:])
