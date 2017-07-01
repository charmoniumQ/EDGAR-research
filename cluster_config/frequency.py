import heapq
import numpy as np
from operator import or_, add
import collections
from cluster_config.spark import spark_cache, make_sc
from cluster_config.download import Status, get_items, filter_status
from util.paragraphs import to_paragraphs, group_paragraphs, \
    to_sentences, to_clauses
from util.stem import stem_list, regularize
from util.spark_utils import key, val


# TODO: n-gram frequency
@spark_cache('gs://arcane-arbor-7359/', './transfer/')
def frequency_pairs(year, qtr, item):
    def key_val(record):
        #key = (record['index']['year'], record['index']['qtr'], record['index']['CIK'])
        key = (2016, 1, record['index']['CIK'])
        val = record['item']
        return (key, val)

    def to_groups_(key, paragraphs):
        groups = group_paragraphs(paragraphs)
        for i, (header, body) in enumerate(groups):
            # TODO: Don't join sentences.
            # becase cleaner bigrams if you leave sentence boundaries in place
            section = ' '.join([header] + body)
            new_key = key + (i,)
            return (new_key, section)

    return (
        get_items(year, qtr, item)              # [record]
        .repartition(150)
        .filter(filter_status(Status.SUCCESS))  # [record]
        .map(key_val)                           # [((year, CIK), rf_text)]
        .flatMapValues(to_sentences)            # [(key, sentence)]
        .flatMapValues(to_clauses)              # [(key, clause)]
        .mapValues(regularize)                  # [(key, clause)]
        .flatMapValues(stem_list)               # [(key, word)]
        .groupByKey()                           # [(key, [word])]
        .mapValues(collections.Counter)
    )


def vectorize(frequency_pairs):
    records = (
        frequency_pairs
        .map(key)
        .collect()
    )
    words = sorted(list(
        frequency_pairs
        .map(val)
        .map(lambda counter: set(counter))
        .treeReduce(or_)
    ))
    matrix = np.array(
        frequency_pairs
        .map(val)
        .map(lambda counter: np.array([counter[word] for word in words]))
        .collect()
    )
    print(sum(map(len, words)), matrix.shape)
    return records, words, matrix


def frequency_average(year, qtr, item, path):
    counter = (
        frequency_pairs(year, qtr, item)
        .map(val)
        .treeReduce(add)
    )
    sum_ = sum(counter.values())
    fname = path / 'freq_{year}-qtr{qtr}-{docs}.csv'.format(**locals())
    with fname.open('w') as f:
        for i, (word, n) in enumerate(counter.most_common()):
            freq = n / sum_
            print('{i:d},{freq:.12e},{word}'.format(**locals()), file=f)


if __name__ == '__main__':
    make_sc('frequency')
    year, qtr, item = 2006, 1, '1a'
    # frequency_average(year, qtr, item)
    data = vectorize(frequency_pairs(year, qtr, item))
    np.save('transfer/vec_doc', data)
