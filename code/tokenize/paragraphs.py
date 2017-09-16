import functools
from nltk.tokenize.punkt import PunktSentenceTokenizer


def paragraph2sentences(paragraph, punkt=None):
    '''Returns a list of sentences from the paragraph'''
    if punkt is None:
        punkt = PunktSentenceTokenizer(train_text=paragraph)
    return punkt.tokenize(paragraph)


def to_sentences(paragraphs_):
    '''Returns a list of sentences for each paragraph'''
    corpus = '\n\n'.join(paragraphs_)
    punkt = PunktSentenceTokenizer(train_text=corpus)
    paragraphs2sentences = functools.partial(paragraph2sentences, punkt=punkt)
    return map(paragraphs2sentences, paragraphs_)


__all__ = ['to_sentences']
