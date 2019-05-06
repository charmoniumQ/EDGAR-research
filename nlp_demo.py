from nltk.util import ngrams
import spacy
nlp = spacy.load("en")
import nltk
import itertools
import collections
from pathlib import Path
import numpy as np

print_phrases = False
print_semantic_terms = False
print_n_grams = True

def concat_lists(lists):
    ret = []
    for list_ in lists:
        ret.extend(list_)
    return ret

def merge_dicts(dicts):
    ret = {}
    for dict_ in dicts:
        ret.update(dict_)
    return ret

def extract_gutenberg(gutenberg_text):
    start_marker = '*** START OF THIS PROJECT GUTENBERG EBOOK A MODEST PROPOSAL ***'
    end_marker = 'End of the Project Gutenberg EBook'    
    return data_raw.split(start_marker)[1].split(end_marker)[0]

def eliminate_wordwrap(text):
    return (
        text
        .replace('\r', '') # carriage returns are redundant
        .replace('\n\n', '<BR />') # save paragraph breaks
        .replace('\n', ' ') # eliminate word wrapping
        .replace('<BR />', '\n') # restore paragraph breaks
    )

def text2paragraphs(text):
    def is_paragraph(chunk):
        return len(chunk) > 50
    return list(filter(is_paragraph, text.split('\n')))

if __name__ == '__main__':
    input_path = Path('corpus.txt')

    # if not input_path.exists():
    #     import requests
    #     raw_text = requests.get('https://www.gutenberg.org/cache/epub/1080/pg1080.txt').text
    #     clean_text = eliminate_wordwrap(extract_gutenberg(raw_text))
    #     with input_path.open('w') as f:
    #         f.write(clean_text)
    with input_path.open('r') as f:
        text = f.read()

    paragraphs = list(map(nlp, text2paragraphs(text)))
    paragraphs = paragraphs[:10]
    paragraphs_entities = [
        [
            tuple([
                token.lemma_
                for token in list(paragraph)[ent.start:ent.end]
            ])
            for ent in paragraph.ents
        ]
        for paragraph in paragraphs
    ]
    paragraphs_phrases = [
        [token for token in paragraph.sents]
        for paragraph in paragraphs
    ]
    if print_phrases:
        print(
            '\n'.join(
                ' '.join(
                    '-'.join(
                        token.orth_ for token in phrase
                    )
                    for phrase in paragraph_phrases
                )
                for paragraph_phrases in paragraphs_phrases
            )
        )
    semantic_tags = set('JJ JJR JJS NN NNP NNPS NNS VB VBD VBG VBN VBP VBZ'.split(' '))
    semantic_tags |= set('RB RBR RBS RP FW'.split(' '))
    paragraphs_phrases_stems = [
        [
            [
                (token.lemma_, token.orth_, token.tag_)
                for token in phrase
                if token.tag_ in semantic_tags and not token.is_stop
            ]
            for phrase in paragraph_phrases
        ]
        for paragraph_phrases in paragraphs_phrases
    ]

    if print_semantic_terms:
        print(
            '\n'.join(
                ' '.join(
                    '-'.join(
                        stem for stem, word, tag in phrase
                    )
                    for phrase in paragraph_phrases_stems
                )
                for paragraph_phrases_stems in paragraphs_phrases_stems
            )
        )
    stem2words = collections.defaultdict(collections.Counter)
    for paragraph_phrases_stems in paragraphs_phrases_stems:
        for phrase in paragraph_phrases_stems:
            for stem, word, pos in phrase:
                stem2words[stem][word] += 1
    def stem2word(stem):
        return stem2words[stem].most_common(1)[0][0]
    max_n = 2
    paragraphs_ns_counts = [
        merge_dicts([
            {
                n: collections.Counter(concat_lists([
                    ngrams([
                        stem for stem, word, pos in phrase
                    ], n)
                    for phrase in paragraph_phrases_stems
                ]))
                for n in range(1, max_n + 1)
            },
            {
                0: collections.Counter({
                    (): sum(map(len, paragraph_phrases_stems))
                })
            }
        ])
        for paragraph_phrases_stems in paragraphs_phrases_stems
    ]
    ns_counts = {
        n: sum([
            paragraph_ns_counts[n]
            for paragraph_ns_counts in paragraphs_ns_counts
        ], collections.Counter())
        for n in range(1, max_n + 1)
    }

    if print_n_grams:
        print(
            '\n\n'.join(
                '\n'.join(
                    f'{count:3d} ' + '-'.join(
                        stem2word(stem) for stem in ngram
                    )
                    for ngram, count in ns_counts.most_common(5)
                )
                for n, ns_counts in ns_counts.items()
            )
        )
