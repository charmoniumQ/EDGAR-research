import copy
import collections
import itertools
from ..util import generator_to_list
from .text import text2paragraphs
from .paragraphs import paragraph2sentences_func
from .sentences import sentence2clauses
from .clauses import clause2words
from .word import word2stem
from .groups import paragraphs2groups


@generator_to_list
def paragraphs2word_stems(paragraphs):
    for paragraph in paragraphs:
        for sentence in paragraph:
            for clause in sentence2clauses(sentence):
                for word in clause2words(clause):
                    stem = word2stem(word)
                    yield word, stem


def count(words_and_stems):
    word_count = collections.Counter()
    stem_count = collections.Counter()
    for word, stem in words_and_stems:
        word_count[word] += 1
        stem_count[stem] += 1
    return word_count, stem_count


@generator_to_list
def text2section_word_stems(text):
    p2s = paragraph2sentences_func(text)
    paragraphs = [p2s(paragraph) for paragraph in text2paragraphs(text)]
    for heading_paragraphs, body_paragraphs in paragraphs2groups(paragraphs):
        heading_word_stems = count(paragraphs2word_stems(heading_paragraphs))
        body_word_stems = count(paragraphs2word_stems(body_paragraphs))
        yield heading_word_stems, body_word_stems


def combine_counts(sections):
    word_count = collections.Counter()
    stem_count = collections.Counter()
    for section in sections:
        ((header_wc, header_sc), (section_wc, section_sc)) = section
        word_count += header_wc + section_wc
        stem_count += header_sc + section_sc
    return word_count, stem_count


def text2sections_and_counts(text):
    p2s = paragraph2sentences_func(text)
    paragraphs = [p2s(paragraph) for paragraph in text2paragraphs(text)]
    for heading_paragraphs, body_paragraphs in paragraphs2groups(paragraphs):
        stem_count = collections.Counter()

        for word, stem in paragraphs2word_stems(heading_paragraphs):
            stem_count[stem] += 1
        for word, stem in paragraphs2word_stems(body_paragraphs):
            stem_count[stem] += 1

        yield heading_paragraphs, body_paragraphs, stem_count
