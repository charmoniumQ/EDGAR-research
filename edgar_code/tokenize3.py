from typing import Dict, List, Tuple
import collections
import re
import spacy
import nltk
from nltk.util import ngrams
from edgar_code.util import merge_dicts, concat_lists


nlp = spacy.load('en_core_web_sm')


def is_paragraph(line: str) -> bool:
    return sum(ch.isalpha() for ch in line) > 100


semantic_tags = set('JJ JJR JJS NN NNP NNPS NNS VB VBD VBG VBN VBP VBZ'.split(' '))
semantic_tags |= set('RB RBR RBS RP FW'.split(' '))
def paragraph2terms(
        paragraph: str
) -> Tuple[Dict[Tuple[str, ...], int], Dict[int, Dict[Tuple[str, ...], int]]]:
    doc = nlp(paragraph)

    entities = collections.Counter([
        tuple(token.orth_ for token in entity)
        for entity in doc.ents
    ])

    phrases = [
        [
            token for token in phrase
            if (
                    token.tag_ in semantic_tags and
                    not token.is_stop and token.is_alpha
            )
        ]
        for phrase in doc.sents
    ]

    stem2words: Dict[str, Dict[str, int]] = \
        collections.defaultdict(collections.Counter)
    for phrase in phrases:
        for token in phrase:
            stem2words[token.lemma_][token.orth_] += 1

    max_n = 3
    ns_counters: Dict[int, Dict[Tuple[str, ...], int]] = merge_dicts([
        {
            n: collections.Counter(concat_lists([
                ngrams([
                    token.lemma_ for token in phrase
                ], n)
                for phrase in phrases
            ]))
            for n in range(1, max_n + 1)
        },
        {
            0: collections.Counter({
                (): sum(map(len, phrases))
            }),
        },
    ])

    return entities, {n: dict(counter) for n, counter in ns_counters.items()}
