from ..util import lol
from . import text
from . import paragraphs
from . import sentence
from . import clause
from . import word


def stem_text(text_):
    '''Inputs text outputs a list of list of list of lists of stems

Each section is a list of paragraphs
Each paragraph is a list of sentences
Each sentence is a list of clauses
Each clause is a list of words
Each word gets turned into a stem
'''
    # TODO: test lazy vs eager evaluation here
    # TODO: test lol.lmap against nested for-loops
    paragraphs_ = text.to_paragraphs(text_)
    paragraphs_ = lol.llist(paragraphs_)

    sentences = paragraphs.to_sentences(paragraphs_)
    sentences = lol.llist(sentences)

    clauses = lol.lmap(sentence.to_clauses, sentences, 2)
    clauses = lol.llist(clauses)

    words = lol.lmap(clause.to_words, clauses, 3)
    words = lol.llist(words)

    stems = lol.lmap(word.to_stem, words, 4)
    stems = lol.llist(stems)

    return stems


if __name__ == '__main__':
    from code.retrieve.main import download_all
    import collections
    c = collections.Counter()
    for record, rf in download_all(2014, 1, '10-K', 'Item 1A').take(1):
        for paragraph in stem_text(rf):
            for sentence_ in paragraph:
                for clause_ in sentence_:
                    c.update(clause_)
    print(c.most_common(10))
    for word_, i in c.most_common(100):
        print('{i: 4d} {word_}'.format(**locals()))
