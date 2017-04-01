import re
import operator
from collections import Counter
from cluster_config import timer, init_spark

sc = init_spark.get_sc('frequency')
year = 2015
qtr = 1
docs = 1000
n_words = None
input = 'gs://output-bitter-voice/10k_data-{year}-qtr{qtr}-{docs}'.format(**locals())

def normalize(text):
    text = text.lower()
    text = re.sub('[^a-z ]', '', text)
    return text

def words(text):
    i = 0
    while i < len(text):
        j = text.find(' ', i)
        if j == -1:
            yield text[i:]
            raise StopIteration()
        yield text[i:j]
        i = j + 1

def get_frequency(info):
    if '1A' in info:
        c = Counter(words(normalize(info['1A'])))
        return c
    else:
        return Counter()

with timer.timer(print_=True):
    risk_data = sc.pickleFile(input)
    counter = risk_data.map(get_frequency).reduce(operator.add)
    sum_ = sum(counter.values()) 
    with open('freq_{year}-qtr{qtr}-{docs}.csv'.format(**locals()), 'w') as f:
        for i, (word, n) in enumerate(counter.most_common(n_words)):
            freq = n / sum_
            print('{i:d},{freq:.12e},{word}'.format(**locals()), file=f)

# gcloud compute copy-files --zone="$zone" "${master}:freq_2015-qtr1-1000.csv" "../results/result_m/"
