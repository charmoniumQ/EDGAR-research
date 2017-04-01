import glob
import pyspark
from haikunator import Haikunator

my_sc = None

def get_sc(name):
    global my_sc
    if my_sc is None:
        name += Haikunator.haikunate(0)
        print(name)
        conf = pyspark.SparkConf().setAppName(name)
        my_sc = pyspark.SparkContext(conf=conf)
        for egg in glob.glob('*.egg'):
            my_sc.addPyFile(egg)
        return my_sc
    else:
        return my_sc
