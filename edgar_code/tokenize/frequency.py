import copy
import collections


def count(stemmed_text):
    count = collections.Counter()
    stems_from = collections.defaultdict(set)
    for word, stem in stemmed_text:
        count[stem] += 1
        stems_from[word].add(stem)
    return count, stems_from



def combine_count(a, b):
    count_a, stems_from_a = a
    count_b, stems_from_b = b
    count = count_a + count_b
    stems_from = copy.deepcopy(stems_from_a)
    for word, stems in stems_from_b.items():
        stems_from[word] |= stems
    return count, stems_from
