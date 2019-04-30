import dask.bag
import itertools
import logging
logging.basicConfig(level=logging.INFO)
from memory_profiler import profile
# this doesn't work because the code is being loaded from an egg, not the filesystem
#profile = lambda x: x
import collections
from tqdm import tqdm
from edgar_code.util import invert, generator2iterator
from edgar_code.util.time_code import time_code
from edgar_code.retrieve import rfs_for
from edgar_code.data.events import events
from edgar_code.data.stop_stems import stop_stems
from edgar_code.tokenize2.main import text2paragraphs, text2ws_counts
from edgar_code.cloud import cache_path, results_path, BagStore, copy, map_const
from edgar_code.util.cache import Cache, DirectoryStore, FileStore


@Cache.decor(BagStore.create(cache_path / 'bags'), miss_msg=True, hit_msg=True)
def section_word_stems_for(year, qtr):
    # this is the cluster-compting part
    def is_good_rf(rf):
        return len(rf) > 1000
    def index_to_keys(oldkey):
        index, n = oldkey
        return (index.year, index.CIK, n)
    def acceptable_doc(doc):
        # at least this many important words to be acceptable
        return len(doc[0]) > 50
    return (
        rfs_for(year, qtr)
        .filter_values(is_good_rf)
        .map_values(text2paragraphs)
        .flatten_values()
        .map_keys(index_to_keys)
        .map_values(text2ws_counts)
        .filter_values(acceptable_doc)
    )

take_frac = 0.18


# this is a DirectoryStore because a file store would be loaded eagerly when this module is imported
# which is not strictly necessary.

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
def compute_some_words(year, qtr):
    def perpartition(partition):
        keys = []
        paragraph_lengths = []
        unstem_map = collections.defaultdict(collections.Counter)
        # TODO: unstem_map in section_word_stems_for should be counter
        counter = collections.Counter()
        for (year, CIK, paragraph_no), (counter_i, unstem_map_i, length_i) in partition:
            keys.append((year, CIK, paragraph_no))
            paragraph_lengths.append(length_i)
            for stem, words_set in unstem_map_i.items():
                unstem_map[stem].update(words_set)

            # TODO: remove stop_stems in word worker, not here
            # remove late stop_stems
            for stop_stem in stop_stems & counter_i.keys():
                del counter_i[stop_stem]

            counter += counter_i
        return keys, paragraph_lengths, unstem_map, counter

    def aggregate(result):
        keys = []
        paragraph_lengths = []
        unstem_map = collections.defaultdict(collections.Counter)
        counter = collections.Counter()
        for keys_i, paragraph_lengths_i, unstem_map_i, counter_i in result:
            keys.extend(keys_i)
            paragraph_lengths.extend(paragraph_lengths_i)
            for stem, words_counter in unstem_map_i.items():
                unstem_map[stem] += words_counter
            counter += counter_i
        return keys, paragraph_lengths, unstem_map, counter

    return section_word_stems_for(year, qtr).reduction(perpartition, aggregate).compute()

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
def compute_all_words():
    def aggregate(result):
        keys = []
        paragraph_lengths = []
        unstem_map = collections.defaultdict(collections.Counter)
        counter = collections.Counter()
        for keys_i, paragraph_lengths_i, unstem_map_i, counter_i in result:
            keys.extend(keys_i)
            paragraph_lengths.extend(paragraph_lengths_i)
            for stem, words_counter in unstem_map_i.items():
                unstem_map[stem] += words_counter
            counter += counter_i
        return keys, paragraph_lengths, unstem_map, counter
    results = []
    for year in range(2006, 2019):
        for qtr in range(1, 5):
            with time_code(f'compute_some_words {year} {qtr}'):
                results.append(compute_some_words(year, qtr))
    keys, paragraph_lengths, unstem_map, counter = aggregate(results)

    stats_for_counter(counter)
    stats_for_unstem(unstem_map)

    words = set(word for word, count in counter.most_common(int(len(counter) * take_frac)))

    # sorting is not strictly necessary
    int2word = dict(sorted(enumerate(list(words))))
    word2int = invert(int2word)
    print(f'{len(keys)} para x {len(word2int)} words')

    return keys, unstem_map, paragraph_lengths, int2word, word2int

# can't cache this if generator
# must be generator because won't fit in memory
#@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True)
@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
def corpus_for(year):
    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()

    def get_bow(value):
        counter, unstem_map, length = value
        return [(word2int[word], count) for word, count in counter.items() if word in word2int]

    corpus2 = []
    for qtr in range(1, 5):
        corpus2.extend(list(
            section_word_stems_for(year, qtr)
            .values()
            .map(get_bow)
        ))
    return corpus2

@generator2iterator
def corpus():
    with time_code('big_corpus'):
        for year in range(2006, 2019):
            with time_code(f'corpus_for {year}'):
                yield from corpus_for(year)

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
def compute_tfidf():
    from gensim.models.tfidfmodel import TfidfModel

    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()
    with time_code('compute_tfidf'):
        tfidf = TfidfModel(corpus, smartirs='ltc', id2word=int2word)
    return tfidf

import os
@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
def compute_lda():
    # from gensim.models.ldamulticore import LdaMulticore
    from gensim.models.lsimodel import LsiModel

    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()

    try:
        len(corpus)
    except:
        for doc in iter(corpus):
            pass

    host = os.environ.get('pyro_ns_host', None)
    port = int(os.environ.get('pyro_ns_port', 0)) or None

    tfidf = compute_tfidf()
    with time_code('compute_lda'):
        corpus_tfidf = tfidf[corpus]
        lda = LsiModel(corpus_tfidf, num_topics=500, id2word=int2word, distributed=True, ns_conf=dict(
            host=host, port=port, broadcast=port and host,
        ))
        # lda = LdaMulticore(corpus_tfidf, num_topics=500, id2word=int2word, workers=None)

    return lda


import csv
from edgar_code.tokenize2.word import word2stem
import numpy as np
def main_sub4():
    '''queries topics for events'''
    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()
    tfidf = compute_tfidf()
    lda = compute_lda()
    topic_mat = lda.get_topics()

    with (results_path / 'all_topics.csv').open('w') as filw:
        csvw = csv.writer(filw)
        n_topics, n_words = topic_mat.shape
        for topic_no in range(n_topics):
            topic = topic_mat[topic_no]
            word_ids = np.argsort(topic)[-50:][::-1]
            row = [f'{topic_no:03d}']
            for word_id in word_ids:
                row.append(f'{topic[word_id]:.02f}')
                row.append(int2word[word_id])
            csvw.writerow(row)

    with (results_path / 'query_topics.csv').open('w') as filw:
        csvw = csv.writer(filw)
        for paragraph in events:
            counter = text2ws_counts(paragraph)[0]
            bow = [(word2int[word], count) for word, count in counter.items() if word in word2int]
            query_topics = lda[tfidf[bow]]
            query_topics = sorted(query_topics, key=lambda pair: -pair[1])
            row = [paragraph[:25]]
            for topic_no, val in query_topics[:10]:
                row.append(f'{topic_no:03d}')
                row.append(f'{val:.2f}')
            csvw.writerow(row)

# TODO: remove
n_topics, n_words = (500, 10203)

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
def compute_year_topic2(year, qtr):

    def perpartition(partition):
        topic_weighted, topic_unweighted, weights, docs = \
            np.zeros(n_topics), np.zeros(n_topics), 0, 0
        for topic_vec, weight in partition:
            for topic, val in topic_vec:
                topic_weighted[topic] += val * weight
                weights += weight
                topic_unweighted[topic] += val
                docs += 1
        return topic_weighted, topic_unweighted, weights, docs

    def aggregate(partitions):
        topic_weighted, topic_unweighted, weights, docs = \
            np.zeros(n_topics), np.zeros(n_topics), 0, 0
        for topic_weighted_i, topic_unweighted_i, weights_i, docs_i in partitions:
            topic_weighted += topic_weighted_i
            topic_unweighted += topic_unweighted_i
            weights += weights_i
            docs += docs_i
        return topic_weighted, topic_unweighted, weights, docs


    if 'word2int' not in globals():
        _, _, globals()['paragraph_lengths'], _, globals()['word2int'] = compute_all_words()
        globals()['tfidf'] = compute_tfidf()

    def mapper(item, lda):
        counter, unstem_map, length = item
        bow = [(word2int[word], count) for word, count in counter.items() if word in word2int]
        tfidf_bow = tfidf[bow]
        vec = lda[tfidf_bow]
        return vec

    stems = section_word_stems_for(year, qtr)
    with time_code(f'compute_year_topic2({year}, {qtr})'):
        return (
            dask.bag.zip(
                map_const(
                    mapper,
                    stems.values(),
                    dask.delayed(compute_lda)(),
                ),
                dask.bag.from_sequence(paragraph_lengths, npartitions=stems.npartitions),
            )
            .reduction(perpartition, aggregate)
            .compute(optimize=True)
        )


def compute_all_year_topics():
    year_topic_weighted, year_topic_unweighted, year_weights, year_docs = \
        np.zeros(((2019-2006) * 4, n_topics)), np.zeros(((2019-2006) * 4, n_topics)), np.zeros(((2019-2006) * 4,)), np.zeros(((2019-2006) * 4,))
    for year in range(2006, 2019):
        for qtr in range(1, 5):
            topic_weighted, topic_unweighted, weights, docs = compute_year_topic2(year, qtr)
            year_topic_weighted[(year - 2006) * 4 + qtr - 1, :] = topic_weighted
            year_topic_unweighted[(year - 2006) * 4 + qtr - 1, :] = topic_unweighted
            year_weights[(year - 2006) * 4 + qtr - 1] = weights
            year_docs[(year - 2006) * 4 + qtr - 1] = docs
    return year_topic_weighted, year_topic_unweighted, year_weights, year_docs

def stats_for_year_topic(year_topic_weighted, year_topic_unweighted, year_weights, year_docs):
    '''plots topics over time'''
    import numpy as np
    import matplotlib.pyplot as plt
    year_topic_avg = year_topic_weighted / year_weights[:, np.newaxis].clip(1, None)
    year_topic_avg2 = year_topic_unweighted / year_docs[:, np.newaxis].clip(1, None)

    def mapper(topic_no):
        # fig = plt.figure(figsize=(5.75, 4))
        # ax = fig.gca()
        # ax.plot(np.arange(2006, 2019, 0.25) - 1, year_topic_avg)
        # ax.set_xticks(np.arange(2006, 2019) - 1)
        # plt.xticks(rotation=35, fontweight='bold')
        # plt.yticks(fontweight='bold')
        # # ax.set_xlabel('Year', fontweight='bold', fontsize=14)
        # ax.set_ylabel('Prevalence', fontweight='bold', fontsize=12)
        # ax.set_title(f'Topic {topic_no}', fontweight='bold', fontsize=12)
        # out_path = f'topic_weighted_{topic_no}.png'
        # fig.savefig(out_path, transparent=True, bbox_inches='tight', dpi=150)
        # copy(out_path, results_path / 'topics' / 'plots' / out_path)

        fig = plt.figure(figsize=(5.75, 4))
        ax = fig.gca()
        ys = [year_topic_avg2[(year - 2006)*4:(year - 2006)*4+4, topic_no].mean() for year in range(2006, 2019)]
        xs = np.arange(2006, 2019, dtype=int) - 1
        ax.plot(xs, ys)
        ax.set_xticks(xs)
        ax.set_xticklabels(xs, dict(fontweight='bold', rotation=35))
        ax.set_yticklabels([label.get_text() for label in ax.get_yticklabels()], dict(fontweight='bold'))
        # ax.set_xlabel('Year', fontweight='bold', fontsize=14)
        ax.set_ylabel('Prevalence', fontweight='bold', fontsize=12)
        ax.set_title(f'Topic {topic_no:03d}', fontweight='bold', fontsize=12)
        out_path = f'topic_unweighted_{topic_no:03d}.png'
        fig.savefig(out_path, transparent=True, bbox_inches='tight', dpi=150)
        plt.close(fig)
        copy(out_path, results_path / 'topics' / 'plots' / out_path)

    (
        dask.bag.range(year_topic_avg.shape[1], npartitions=40)
        .map(mapper)
        .compute()
    )
        

def stats_for_counter(counter):
    '''plots words over time'''
    import numpy as np
    import matplotlib.pyplot as plt

    total_words = sum(counter.values())
    words = np.array(list(counter.keys()))
    freqs = np.array(list(counter.values()))
    m = np.argsort(freqs)[::-1]
    words = words[m]
    freqs = freqs[m]
    ranks = np.arange(len(freqs))
    log_ranks = np.log1p(ranks)
    log_freqs = np.log1p(freqs / total_words)
    with (results_path / 'word_freq.csv').open('w') as f:
        csvw = csv.writer(f)
        for word, freq in zip(words, freqs):
            csvw.writerow([word, freq])

    # fig = plt.figure()
    # ax = fig.gca()
    # ax.plot(ranks, freqs)
    # ax.set_xlabel('rank')
    # ax.set_ylabel('freq')
    # ax.set_title('Word distribution')
    # out_path = f'word_freq.png'
    # fig.savefig(out_path)
    # copy(out_path, results_path / out_path)

    fig = plt.figure()
    ax = fig.gca()
    ax.plot(log_ranks, log_freqs)
    ax.set_xlabel('log(1 + rank)')
    ax.set_ylabel('log(freq)')
    ax.set_title('Word distribution')
    out_path = f'log_word_freq.png'
    fig.savefig(out_path)
    fig.close()
    copy(out_path, results_path / out_path)

def stats_for_unstem(unstem_map):
    with (results_path / 'stems.csv').open('w') as f:
        csvw = csv.writer(f)
        for stem, words_counter in unstem_map.items():
            csvw.writerow([stem] + [str(w) for w in itertools.chain.from_iterable(words_counter.most_common())])


def main():
    # main_sub4()

    stats_for_year_topic(*compute_all_year_topics())


if __name__ == '__main__':
    main()

# docker build -t test edgar_deploy/dockerfiles/job && docker run -it --rm --volume ${PWD}:/w2 --workdir /w2 --memory 500Mb --env GOOGLE_APPLICATION_CREDENTIALS=/w2/edgar_deploy/main-722.service_account.json --env local_test=1 test python3 -m edgar_code.executables.tokenize_rfs

#   File "/home/sam/box/dev/EDGAR-research/edgar_deploy/env/lib/python3.7/site-packages/google/cloud/storage/bucket.py", line 859, in delete
