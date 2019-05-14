from typing import Dict, Tuple
import collections
from typing_extensions import Counter
import spacy
from nltk.util import ngrams
from edgar_code.util import merge_dicts, concat_lists


PhraseCount = Counter[Tuple[str, ...]]
TermsType = Tuple[PhraseCount, Dict[int, PhraseCount], Dict[str, Counter[str]]]

# TODO: make this mapconst
nlp = spacy.load('en_core_web_sm')

def is_paragraph(line: str) -> bool:
    return sum(ch.isalpha() for ch in line) > 100

semantic_entities = set(
    'PERSON NORP FAC ORG GPE LOC PRODUCT EVENT'
    ' WORK_OF_ART LAW LANGUAGE PER LOC ORG MISC'.split(' ')
)
semantic_tags = set('JJ JJR JJS NN NNP NNPS NNS VB VBD VBG VBN VBP VBZ'.split(' '))
semantic_tags |= set('RB RBR RBS RP FW'.split(' '))
def paragraph2terms(paragraph: str) -> TermsType:
    doc = nlp(paragraph)

    entities = Counter([
        tuple(token.lemma_ for token in entity)
        for entity in doc.ents
        if entity.label_ in semantic_entities
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

    stem2words: Dict[str, Counter[str]] = \
        collections.defaultdict(Counter)
    for phrase in phrases:
        for token in phrase:
            stem2words[token.lemma_][token.orth_] += 1
    stem2words = dict(stem2words)

    max_n = 3
    ns_counters: Dict[int, PhraseCount] = merge_dicts([
        {
            n: Counter(concat_lists([
                ngrams([
                    token.lemma_ for token in phrase
                ], n)
                for phrase in phrases
            ]))
            for n in range(1, max_n + 1)
        },
        {
            0: Counter({
                (): sum(map(len, phrases))
            }),
        },
    ])

    return entities, ns_counters, stem2words
