import dask.bag
import itertools
#from memory_profiler import profile
profile = lambda x: x
import gc
import sys
import collections
from edgar_code.util import invert, generator2iterator
from edgar_code.util.time_code import time_code
from edgar_code.retrieve import rfs_for
from edgar_code.data.events import events
from edgar_code.data.stop_stems import stop_stems
from edgar_code.tokenize2.main import text2paragraphs, text2ws_counts
from edgar_code.cloud import cache_path, results_path, BagStore, copy
from edgar_code.util.cache import Cache, DirectoryStore


@Cache.decor(BagStore.create(cache_path / 'bags'), miss_msg=True, hit_msg=True)
def section_word_stems_for(year, qtr):
    # this is the cluster-compting part
    def is_good_rf(rf):
        return len(rf) > 1000
    def index_to_keys(oldkey):
        index, n = oldkey
        return (index.year, index.CIK, n)
    return (
        rfs_for(year, qtr)
        .filter_values(is_good_rf)
        .map_values(text2paragraphs)
        .flatten_values()
        .map_keys(index_to_keys)
        .map_values(text2ws_counts)
    )

take_frac = 0.20


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
        for (year, CIK, paragraph_no), (counter_i, unstem_map_i, length_i, n_words_i) in partition:
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
def big_corpus(really_compute=True):
    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()

    def get_bow(value):
        counter, unstem_map, length, words = value
        return [(word2int[word], count) for word, count in counter.items() if word in word2int]

    @Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
    def corpus_for(year):
        corpus2 = []
        for qtr in range(1, 5):
                corpus2.extend(list(
                    section_word_stems_for(year, qtr)
                    .values()
                    .map(get_bow)
                ))
        return corpus2

    big_corpus.corpus_for = corpus_for

    if not really_compute:
        return

    with time_code('big_corpus'):
        for year in range(2006, 2019):
            with time_code(f'corpus_for {year}'):
                yield from corpus_for(year)

corpus = generator2iterator(big_corpus)

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
@profile
def compute_tfidf():
    from gensim.models.tfidfmodel import TfidfModel

    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()
    with time_code('compute_tfidf'):
        tfidf = TfidfModel(corpus, smartirs='ltc', id2word=int2word)
    return tfidf

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
@profile
def compute_lda():
    from gensim.models.ldamulticore import LdaMulticore

    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()

    try:
        len(corpus)
    except:
        for doc in iter(corpus):
            pass

    tfidf = compute_tfidf()
    with time_code('compute_lda'):
        corpus_tfidf = tfidf[corpus]
        lda = LdaMulticore(corpus_tfidf, num_topics=500, id2word=int2word, workers=None)

    return lda

@Cache.decor(BagStore.create(cache_path / 'cache'), miss_msg=True, hit_msg=True)
@profile
def compute_topics(year):
    tfidf = compute_tfidf()
    lda = compute_lda()
    big_corpus(really_compute=False)
    with time_code(f'compute_topics {year}'):
        return (
            big_corpus.corpus_for(year)
            .map(lambda doc: lda[tfidf[doc]])
        )


import csv
def main_sub4():
    '''queries topics for events'''
    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()
    tfidf = compute_tfidf()
    lda = compute_lda()

    with (results_path / 'topics' / 'all.txt').open('w') as fil:
        n_topics, n_words = lda.get_topics()
        for topic_no in range(n_topics):
            fil.write(f'{topic_no}: {lda.print_topic(topic, topn=30)}\n')

    with (results_path / 'topics' / 'query.txt').open('w') as filw:
        csvw = csv.writer(filw)
        for query_words in events:
            query_corpus = [(word2int[word], count) for word, count in collections.Counter(query_words) if word in word2int]
            query_topics = lda[tfidf[query_corpus]]
            for topic_no, topic in query_topics.print_topics(num_topics=3, num_words=30):
                csvw.writerow(query_words + ['', str(topic_no), ''] + [word for word, value in topic])

@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True)
def compute_year_topic():
    import numpy as np
    keys, unstem_map, paragraph_lengths, int2word, word2int = compute_all_words()
    tfidf = compute_tfidf()
    lda = compute_lda()

    n_topics, n_words = lda.get_topics()
    year_topic = np.zeros((2019-2006, n_topics))
    docs_per_year = np.zeros((2019-2006))
    last_i = 0
    for year in range(2006, 2019):
        for topic_vec, key, weight in zip(compute_topics(year), keys[i:], paragraph_lengths[i:]):
            last_i += 1
            for topic, val in topic_vec:
                year_topic[year - 2006, topic] += val * paragaph_length
                docs_per_year[year - 2006] += 1
    return year_topic, docs_per_year

def stats_for_year_topic(year_topic, docs_per_year):
    '''plots topics over time'''
    import numpy as np
    import matplotlib.pyplot as plt
    year_topic_avg = year_topic / docs_per_year[:, np.newaxis]

    def mapper(topic_no):
        fig = plt.figure()
        ax = fig.axes()
        ax.plot(np.arange(2006, 2019), year_topic_avg)
        ax.set_xticks(np.arange(2006, 2019))
        ax.set_xlabel('Year')
        ax.set_ylabel('Prevalence')
        ax.set_title('Topic {topic_no}')
        out_path = f'topic_{topic_no}.png'
        fig.savefig(out_path)
        copy(out_pat, results_path / 'topics' / 'plots' / out_path)

    (
        dask.bag.range(year_topic.shape[1])
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
    copy(out_path, results_path / out_path)

def stats_for_unstem(unstem_map):
    with (results_path / 'stems.csv').open('w') as f:
        csvw = csv.writer(f)
        for stem, words_counter in unstem_map.items():
            csvw.writerow([stem] + [str(w) for w in itertools.chain.from_iterable(words_counter.most_common())])


def main():
    main_sub4()
    stats_for_year_topic(*compute_year_topic())


if __name__ == '__main__':
    main()

# docker build -t test edgar_deploy/dockerfiles/job && docker run -it --rm --volume ${PWD}:/w2 --workdir /w2 --memory 500Mb --env GOOGLE_APPLICATION_CREDENTIALS=/w2/edgar_deploy/main-722.service_account.json --env local_test=1 test python3 -m edgar_code.executables.tokenize_rfs

#   File "/home/sam/box/dev/EDGAR-research/edgar_deploy/env/lib/python3.7/site-packages/google/cloud/storage/bucket.py", line 859, in delete
