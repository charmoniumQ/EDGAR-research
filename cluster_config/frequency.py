import pyspark
from pathlib import Path
import numpy as np
import operator
import collections
from cluster_config.spark import spark_cache, make_sc
from cluster_config.download import Status, get_items, filter_status
from util.paragraphs import to_paragraphs, group_paragraphs, \
    to_sentences, to_clauses
from util.stem import stem, regularize
from util.spark_utils import key, val


# TODO: n-gram frequency
@spark_cache('gs://arcane-arbor-7359/', './transfer/')
def frequency_pairs(year, qtr, item):
    def key_val(record):
        # key = (record['index']['year'], record['index']['qtr'], record['index']['CIK'])
        key = (2006, 1, record['index']['CIK'])
        val = record['item']
        return (key, val)

    def to_groups_(key, paragraphs):
        groups = group_paragraphs(paragraphs)
        for section_number, (header, body) in enumerate(groups):
            sentences = [header] + body
            new_key = key + (section_number,)
            return (new_key, sentences)

    def n_grams(n):
        def n_grams_(lst):
            for i in range(n+1):
                indices = [(j, len(lst)-n+j+1) for j in range(i)]
                yield from list(zip(*[lst[start:stop] for start, stop in indices]))
        return n_grams_

    def join_n_grams(n_gram):
        return '_'.join(n_gram)

    def stem_list(words):
        yield from map(stem, words)

    good_rfs = (
        get_items(year, qtr, item)            # [record]
        .repartition(50)
        .filter(filter_status(Status.SUCCESS))  # [record]
        .map(key_val)                         # [((year, qtr, CIK), rf_text)]
    )
    sections = True
    if sections:
        rf_paragraphs = (
            good_rfs
            .mapValues(to_paragraphs)        # [((year, qtr, CIK), [paragraphs])]
            .flatMapValues(to_groups_)       # [((year, qtr, CIK, section), [sentence])]
            .flatMapValues()                 # [((year, qtr, CIK, section), sentence)] non-unique
        )
    else:
        rf_paragraphs = (
            good_rfs
            .flatMapValues(to_sentences)      # [((year, qtr, CIK), sentence)] non-unique
        )
    words = (
        rf_paragraphs
        .flatMapValues(to_clauses)            # [(key, clause)] non-unique
        .mapValues(regularize)                # [(key, clause)] non-unique
        .mapValues(tokenize)                  # [(key, [word])] non-unique
        .mapValues(stem_list)                 # [(key, [stem])] non-unique <---- formatting
    )
    n_grams = (
        words
        .flatMapValues(n_grams(2))            # [(key, n_gram)] non-unique
        .mapValues(join_n_grams)              # [(key, word)]
        .groupByKey()                         # [(key, [word])] non-unique <----- formatting
    )
    counts = (
        n_grams
        .mapValues(collections.Counter)       # [((year, qtr, CIK), counter)]
    )
    save(**locals())


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
        .treeReduce(operator.or_)
    ))
    matrix = np.array(
        frequency_pairs
        .map(val)
        .map(lambda counter: np.array([counter[word] for word in words]))
        .collect()
    )
    save(**locals())
    print(sum(map(len, words)), matrix.shape)
    return records, words, matrix


def save(locals):
    for name, var in locals.items():
        path = Path('transfer') / name
        save_(path, var)


def save_(path, thing):
    if isinstance(thing, pyspark.RDD):
        path.mkdir(parents=True)
        for key_, val_ in thing:
            if isinstance(key_, tuple):
                key_ = '_'.join(map(str, key_))
            else:
                key_ = str(key_)
            save_(path / key_, val_)
    elif isinstance(thing, str):
        append = path.exist()
        with path.open('a') as f:
            if append:
                f.write('====\n')
            f.write(thing + '\n')
    elif isinstance(thing, collections.Counter):
        sum_ = sum(thing.values())
        with path.open('w') as f:
            for i, (word, n) in enumerate(thing.most_common()):
                freq = n / sum_
                f.write('{i:d},{n:d},{freq:.12e},{word}\n'.format(**locals()))
    elif isinstance(thing, list):
        append = path.exists()
        with path.open('a') as f:
            if append:
                f.write('====\n')
            for x in thing:
                if isinstance(x, tuple):
                    f.write(','.join(map(str, x)) + '\n')
    elif isinstance(thing, np.ndarray):
        np.save(str(path), thing, delimiter=',')
    else:
        raise NotImplementedError(type(thing) + ' is not supported yet')


if __name__ == '__main__':
    make_sc('frequency')
    year, qtr, item = 2006, 1, '1a'
    # frequency_average(year, qtr, item)
    data = vectorize(frequency_pairs(year, qtr, item))
    np.save('transfer/vec_doc', data)
