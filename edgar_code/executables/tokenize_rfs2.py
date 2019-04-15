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

    words = set(word for word, count in counter.most_common(int(len(counter) * take_frac)))

    # sorting is not strictly necessary
    int2word = dict(sorted(enumerate(list(words))))
    word2int = invert(int2word)
    print(f'{len(keys)} para x {len(word2int)} words')

    return keys, unstem_map, paragraph_lengths, int2word, word2int

import itertools
import numpy as np
import dask.bag
import matplotlib.pyplot as plt
def plot_events():
    words = list(itertools.chain.from_iterable(events))

    def mapper(pair):
        (year, CIK, paragraph_no), (counter_i, unstem_map_i, length_i, n_words_i) = pair
        # TODO: weighting scheme
        vec = np.array([counter_i[word] for word in words])
        return vec

    def sum2(vecs):
        out_vec = np.zeros(len(words))
        for vec in vecs:
            out_vec += vec
        return out_vec

    years = list(range(2006, 2019))
    data = np.zeros((len(years) * 4, len(words)))
    for year in years:
        for qtr in range(1, 5):
            data[(year - 2006) * 4 + qtr, :] = (
                section_word_stems_for(year, qtr)
                .map(mapper)
                .reduction(sum2, sum2)
                .compute()
            )

    def mapper2(pair):
        word_i, word = pair
        xs = np.arange(2006, 2019, 0.25)
        ys = data[:, word_i]

        fig = plt.figure()
        ax = fig.axes()
        ax.plot(xs, ys)
        ax.set_xticks(np.arange(2006, 2019))
        ax.set_xlabel('Year')
        ax.set_ylabel('Prevalence')
        ax.set_title('{word}')
        out_path = f'topic_{word}.png'
        fig.savefig(out_path)
        copy(out_path, results_path / 'topics' / 'plots' / out_path)
        
    (
        dask.bag.from_iterable(enumerate(words))
        .map(mapper2)
        .compute()
    )

def main():
    plot_events()

if __name__ == '__main__':
    main()

# docker build -t test edgar_deploy/dockerfiles/job && docker run -it --rm --volume ${PWD}:/w2 --workdir /w2 --memory 500Mb --env GOOGLE_APPLICATION_CREDENTIALS=/w2/edgar_deploy/main-722.service_account.json --env local_test=1 test python3 -m edgar_code.executables.tokenize_rfs
