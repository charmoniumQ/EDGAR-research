import operator
import collections
from cluster_config.download import Status, get_items, filter_status
from util.paragraphs import to_paragraphs, group_paragraphs
from util.stem import stem_list, regularize


def key_val(record):
    key = (record['index']['year'], record['index']['CIK'])
    val = record['item']
    return (key, val)


def to_groups_(key, paragraphs):
    groups = group_paragraphs(paragraphs)
    for i, (header, body) in enumerate(groups):
        section = ' '.join([header] + body)
        new_key = key + (i,)
        return (new_key, section)


def stats_for(year, qtr, item):
    res1 = (
        get_items(year, qtr, item)
        .filter(filter_status(Status.SUCCESS))
        .map(key_val)
        .mapValues(to_paragraphs)
        .flatMap(to_groups_)
        .mapValues(regularize)
        .mapValues(stem_list)
        .mapValues(collections.Counter)
    )
    res2 = (
        res1
        .map(lambda key, val: val)
        .reduce(operator.add)
        .collect()
        )
    return res


def get_frequency(year, qtr, item, n_words):
    counter = stats_for(year, qtr, item)
    sum_ = sum(counter.values())
    with open('freq_{year}-qtr{qtr}-{docs}.csv'.format(**locals()), 'w') as f:
        for i, (word, n) in enumerate(counter.most_common(n_words)):
            freq = n / sum_
            print('{i:d},{freq:.12e},{word}'.format(**locals()), file=f)


# gcloud compute copy-files --zone="$zone" "${master}:freq_2015-qtr1-1000.csv" "../results/result_m/"
