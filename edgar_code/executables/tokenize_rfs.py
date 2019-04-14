#from memory_profiler import profile
profile = lambda x: x
import gc
import sys
import collections
from edgar_code.util import invert
from edgar_code.util.time_code import time_code
from edgar_code.retrieve import rfs_for
from edgar_code.tokenize2.main import text2paragraphs, text2ws_counts
from edgar_code.cloud import cache_path, results_path
from edgar_code.util.cache import Cache, DirectoryStore, FileStore


@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True)
@profile
def section_word_stems_for(year, qtr):
    # this is the cluster-compting part
    def is_good_rf(rf):
        return len(rf) > 1000
    def index_to_keys(oldkey):
        index, n = oldkey
        return (index.year, index.CIK, n)
    with time_code(f'section_stems_for({year}, {qtr})'):
        l = list(
            rfs_for(year, qtr)
            .filter_values(is_good_rf)
            .map_values(text2paragraphs)
            .flatten_values()
            .map_keys(index_to_keys)
            .map_values(text2ws_counts)
        ) # List[((year, CIK, paragraph_no), (counter, unstem))]
        return l


# this is a DirectoryStore because a file store would be loaded eagerly when this module is imported
# which is not strictly necessary.
@Cache.decor(DirectoryStore.create(cache_path / 'cache'), miss_msg=True)
@profile
def main_sub1():
    keys = []
    unstem_map = collections.defaultdict(set)
    # paragraph_words = []
    paragraph_lengths = []
    all_words = set()
    word_count_low_cutoff = 1
    with time_code('main_sub1 loop'):
        for year in range(2006, 2019):
            for qtr in range(1, 5):
                for (year, CIK, paragraph_no), (counter_i, unstem_i, length_i, words_i) in section_word_stems_for(year, qtr):
                    # append key to key map
                    keys.append((year, CIK, paragraph_no))

                    # I need all words to do tf/idf
                    all_words |= set(word for word, count in counter_i.items() if count > word_count_low_cutoff)

                    # paragraph_words.append(words_i)

                    paragraph_lengths.append(length_i)

                    # combine unstem map
                    for stem, unstems in unstem_i.items():
                        unstem_map[stem] |= unstems
                gc.collect()
                # for x in 'keys unstem_map paragraph_words paragraph_lengths all_words counters'.split(' '):
                #     print(x, sys.getsizeof(locals()[x]))

    with time_code('main_sub1 post'):
        # sorting is not strictly necessary
        int2word = dict(sorted(enumerate(list(all_words))))
        word2int = invert(int2word)
        print(f'{len(keys)} para x {len(word2int)} words')

    return keys, unstem_map, paragraph_lengths, int2word, word2int

@Cache.decor(FileStore.create(cache_path / 'cache'), miss_msg=True)
def main_sub2():
    from gensim.corpora.dictionary import Dictionary
    from gensim.models.tfidfmodel import TfidfModel
    from gensim.models.ldamulticore import LdaMulticore

    keys, unstem_map, paragraph_lengths, int2word, word2int = main_sub1()

    # iterating twice because the counters won't all fit in memory at the same time
    # I need to get all_words first
    # and vectorize documents second
    def corpus_gen():
        for year in range(2006, 2019):
            for qtr in range(1, 5):
                for (year, CIK, paragraph_no), (counter_i, unstem_i, length_i, words_i) in section_word_stems_for(year, qtr):
                    yield [(word2int[word], count) for word, count in counter_i.items() if count > word_count_low_cutoff]
                    # corpus problem

    # (lL) (nt) (c)
    with time_code('main_sub2'):
        corpus = list(corpus_gen())
        tfidf = TfidfModel(corpus, smartirs='ltc', id2word=int2word)
        lda = LdaMulticore(tfidf[corpus], num_topics=500, id2word=int2word)
        corpus_lda = lda[tfidf[corpus]]

    return tfidf, lda, corpus_lda


import csv
def main_sub3():
    keys, unstem_map, paragraph_lengths, int2word, word2int = main_sub1()
    tfidf, lda, corpus_lda = main_sub2()

    with (results_path / 'topics' / 'all.txt').open('w') as fil:
        n_topics, n_words = query.get_topics()
        for topic_no in range(n_topics):
            fil.write(f'{topic_no}: {lda.print_topic(topic, topn=30)}\n')

    with (results_path / 'topics' / 'query.txt').open('w') as filr:
        csvr = csv.reader(filr)
        with open('edgar_code/data/events.txt') as filw:
            csvw = csv.writer(filw)
            for query_words in csvr:
                # corpus problem
                query = lda[tfidf[word2int[query_words]]]
                for topic_no, topic in query.print_topics(num_topics=3, num_words=30):
                    csvw.writerow(query_words + [':'] + [word for word, value in topic])


main = main_sub1


if __name__ == '__main__':
    main()
