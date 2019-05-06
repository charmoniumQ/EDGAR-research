import nltk
import itertools
import collections
from pathlib import Path
import numpy as np
from nltk.util import ngrams

print_poses = False
print_phrases = True
print_semantic_terms = False
print_n_grams = False

def concat_lists(lists):
    return list(itertools.chain.from_iterable(lists))

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

def get_sent_tokenizer(text_sample):
    return nltk.tokenize.PunktSentenceTokenizer(text).tokenize

word_tokenizer = nltk.tokenize.treebank.TreebankWordTokenizer().tokenize
pos_tagger = nltk.tag.perceptron.PerceptronTagger().tag

NOUN_MOD = set('DT CC JJ JJR JJS PRP$ RB RBR RBS VBG PDT WP$ WDT'.split(' '))
NOUN_MOD_WORDS = set('of'.split(' '))
NOUN = set('NN NNS NP NPS PRP WP'.split(' '))
PREP = set('TO IN'.split(' '))
NOT_PREP_WORDS = set('if'.split(' '))
VERB_MOD = set('RB RBR RBS RP'.split(' '))
VERB = set('VBD VBN VBP VBZ MD VB'.split(' '))
def sentence_poses2phrases(sentence_poses):
    phrases = []
    state = 'blank state'
    phrase = []
    sentence_poses = sentence_poses[:]

    def blank_state(word, pos):
        word = word.lower()
        if pos in NOUN_MOD | NOUN or word in NOUN_MOD_WORDS:
            return ('noun phrase', [('', 'np'), (word, pos)])
        elif pos in VERB_MOD | VERB:
            return ('verb phrase', [('', 'vp'), (word, pos)])
        elif pos in PREP and word not in NOT_PREP_WORDS:
            return ('prep phrase', [('', 'pp'), (word, pos)])
        else:
            phrases.append([('unk', word)])
            return ('blank state', [])
    for word, pos in sentence_poses:
        # print(state, phrase, word, pos,
        #       ' '.join(filter(bool, [
        #           part if pos in globals()[part] else ''
        #           for part in 'NOUN_MOD NOUN PREP VERB_MOD VERB'.split(' ')
        #       ]))
        # )
        if state == 'blank state':
            state, phrase = blank_state(word, pos)
            continue
        elif state == 'noun phrase' or state == 'prep phrase':
            if pos in NOUN_MOD | NOUN or word in NOUN_MOD_WORDS:
                phrase.append((word, pos))
            else:
                phrases.append(phrase)
                state, phrase = blank_state(word, pos)
            continue
        elif state == 'verb phrase':
            if pos in VERB | VERB_MOD:
                phrase.append((word, pos))
            else:
                phrases.append(phrase)
                state, phrase = blank_state(word, pos)
            continue
        else:
            raise ValueError('unknown state')
    return phrases

# grammar = r"""
# PARTICIPLE: {<VBG> <DT>?}
# NOUN_PHRASE: {<.?DT>? (<JJ.?|PARTICIPLE|RB|POS|CD|PRP\$|NN.?|NP.?|PRP> <CC>)? <JJ.?|PARTICIPLE|RB|POS|CD|PRP\$|NN.?|NP.?|PRP>* <NN.?|NP.?|PRP>}
# PREP_PHRASE: {<IN|TO> <.?DT>? (<JJ.?|PARTICIPLE|RB|POS|CD|PRP\$|NN.?|NP.?|PRP|NN.?|NP.?|PRP> <CC>)? <JJ.?|PARTICIPLE|RB|POS|CD|PRP\$|NN.?|NP.?|PRP>* <NN.?|NP.?|PRP>}
# VERB_PHRASE: {<IN|TO|RP|RB.?|VB.?|MD>* <VB.?|MD> <RP|RB.?>*}
# """
# phrase_parser = nltk.RegexpParser(grammar).parse
# interesting_phrases = {'VERB_PHRASE', 'NOUN_PHRASE', 'PREP_PHRASE'}
# def sentence_poses2phrases(sentence_poses):
#     return [
#         subtree.leaves()
#         for subtree in phrase_parser(sentence_poses).subtrees(lambda t: t.label() in interesting_phrases)
#     ]

# https://repository.upenn.edu/cgi/viewcontent.cgi?article=1603&context=cis_reports
semantic_pos = set('JJ JJR JJS NN NNS NP NPS VB VBD VBG VBN VBP VBZ RB RBR RBS'.split(' '))
def is_semantic_word(word, pos):
    return pos in semantic_pos and nltk.corpus.wordnet.synsets(word)

# stemmer = nltk.stem.wordnet.WordNetLemmatizer()
stemmer =  nltk.stem.porter.PorterStemmer()

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

    paragraphs = text2paragraphs(text)
    paragraphs = paragraphs[:5]
    # text_sample = '\n'.join(random.sample(paragraphs, len(paragraphs) // 10))
    text_sample = text
    sent_tokenizer = get_sent_tokenizer(text)
    paragraphs_sentences = [
        sent_tokenizer(string)
        for string in paragraphs
    ]
    paragraphs_sentences_words = [
        [
            word_tokenizer(sentence)
            for sentence in paragraph_sentences
        ]
         for paragraph_sentences in paragraphs_sentences
    ]
    paragraphs_sentences_poses = [
        [
            pos_tagger(sentence_words)
            for sentence_words in paragraph_sentences_words
        ]
        for paragraph_sentences_words in paragraphs_sentences_words
    ]
    if print_poses:
        print(
            '\n\n'.join(
                '\n'.join(
                    ' '.join(
                        f'{word}/{pos}' for word, pos in sentence_poses                        
                    )
                    for sentence_poses in paragraph_sentences_poses
                )
                for paragraph_sentences_poses in paragraphs_sentences_poses
            )
        )
    paragraphs_phrases = [
        concat_lists([
            sentence_poses2phrases(sentence_poses)
            for sentence_poses in paragraph_sentences_poses
        ])
        for paragraph_sentences_poses in paragraphs_sentences_poses
    ]
    if print_phrases:
        print(
            '\n'.join(
                ' '.join(
                    '-'.join(
                        (
                            f'{word}/{pos}' if word else f'{pos}'
                        )
                        for word, pos in phrase
                    )
                    for phrase in paragraph_phrases
                )
                for paragraph_phrases in paragraphs_phrases
            )
        )

    max_n = 2
    paragraphs_phrases_stems = [
        [
            [
                (stemmer.stem(word), word, pos)
                for word, pos in phrase
                if is_semantic_word(word, pos)
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
                        stem for stem, word, pos in phrase
                    )
                    for phrase in paragraph_phrases_stems
                )
                for paragraph_phrase_stems in paragraphs_phrases_stems
            )
        )
    stem2words = collections.defaultdict(collections.Counter)
    for paragraph_phrases_stems in paragraphs_phrases_stems:
        for phrase in paragraph_phrases_stems:
            for stem, word, pos in phrase:
                stem2words[stem][word] += 1
    def stem2word(stem):
        return stem2words[stem][word].most_common(1)[0][0]
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
                    f'{count:3d}' + '-'.join(
                        stem2word(stem) for stem in ngram
                    )
                    for ngram, count in ns_counts.most_common(5)
                )
                for n, ns_counts in ns_counts.items()
            )
        )

    # def penntag2wordnettag(penntag, returnNone=False):
    #     return {
    #         'NN': nltk.corpus.wordnet.NOUN,
    #         'JJ': nltk.corpus.wordnet.ADJ,
    #         'VB': nltk.corpus.wordnet.VERB,
    #         'RB': nltk.corpus.wordnet.ADV
    #     }.get(penntag[:2], None if returnNone else '')

    # def my_lesk(context, word, penntag):
    #     wordnettag = penntag2wordnettag(penntag)
    #     if not wordnettag:
    #         # we have a DT or punctuation
    #         # don't even try to wsd
    #         return None
    #     else:
    #         return nltk.wsd.lesk(context, word, wordnettag)

    # paragraphs_phrases_synsets = []
    # for paragraph_phrases in paragraphs_phrases:
    #     paragraph_words = [
    #         word
    #         for phrase in paragraph_phrases
    #         for word, pos in phrase
    #         if pos in semantic_pos
    #     ]
    #     paragraph_phrases_synsets = []
    #     for phrase in paragraph_phrases:
    #         phrase_synsets = []
    #         for word, pos in phrase:
    #             synset = my_lesk(paragraph_words, word, pos)
    #             if synset:
    #                 phrase_synsets.append(synset)
    #         paragraph_phrases_synsets.append(phrase_synsets)
    #     paragraphs_phrases_synsets.append(paragraph_phrases_synsets)

    # if print_semantic_terms:
    #     for paragraph_phrases_synsets in paragraphs_phrases_synsets:
    #         for phrase in paragraph_phrases_synsets:
    #             for synset in phrase:
    #                 print(synset.name(), end=' ')
    #         print('\n')

    # def is_significant_synset(synset, word, pos):
    #     # if wordnet could not find synset (especially if the pos tag was not a noun, adj, adv, or verb), then this probably is not semantically meaningful
    #     # additionally, I have identified certain stop synsets
    #     return synset is not None and synset.lemma() != 'be'

    # all_sentences_synsets = [
    #     [
    #         synset
    #         for synset, word, pos in sentence_synsets
    #         if is_significant_synset(synset, word, pos)
    #     ]
    #     for paragraph_sentences_synsets in paragraphs_sentences_synsets
    #     for sentence_synsets in paragraph_sentences_synsets
    # ]

    # synset2word = collections.defaultdict(collections.Counter)

    # for paragraph_sentences_synsets in paragraphs_sentences_synsets:
    #     for sentence_synsets in paragraph_sentences_synsets:
    #         for synset, word, pos in sentence_synsets:
    #             synset2word[synset][word] += 1

    # synset_ngrams_counter = {
    #     n: collections.Counter([
    #         synset_ngram
    #         for sentence_synsets in all_sentences_synsets
    #         for synset_ngram in ngrams(sentence_synsets, n)
    #     ])
    #     for n in [1, 2, 3]
    # }

    # # Every token is part of the same 0-long N-gram
    # synset_ngrams_counter[0] = collections.Counter({
    #     (): sum(len(sentence_synsets) for sentence_synsets in all_sentences_synsets)
    # })
    # # this makes the NgramAssocMeasures more pretty

    # if print_n_grams:
    #     for n, synset_ngram_counter in synset_ngrams_counter.items():
    #         for synset_ngram, count in synset_ngram_counter.most_common(5):
    #             print(' '.join(synset2word[synset].most_common(1)[0][0] for synset in synset_ngram), count)

    # from nltk.metrics.association import BigramAssocMeasures

    # n_bigrams = len(synset_ngrams_counter[2])
    # for bigram, count in synset_ngrams_counter[2].most_common():
    #     if count >= 2 or True:

    #         score = BigramAssocMeasures.likelihood_ratio(
    #             synset_ngrams_counter[2][(bigram[0], bigram[1])],
    #             (
    #                 synset_ngrams_counter[1][(bigram[0],)],
    #                 synset_ngrams_counter[1][(bigram[1],)],
    #             ),
    #             synset_ngrams_counter[0][()],
    #         )
    #         if score > 7.879:
    #             print([synset2word[synset].most_common(1)[0][0] for synset in bigram], score)
    #     else:
    #         break
