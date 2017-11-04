from ..util import generator_to_list
from .text import text2paragraphs
from .paragraphs import paragraph2sentences_func
from .sentences import sentence2clauses
from .clauses import clause2words
from .word import word2stem


@generator_to_list
def stem_text(text):
    '''Inputs text outputs a list of list of list of lists of stems

Each section is a list of paragraphs
Each paragraph is a list of sentences
Each sentence is a list of clauses
Each clause is a list of words
Each word gets turned into a stem
'''
    paragraph2sentences = paragraph2sentences_func(text)
    for paragraph in text2paragraphs(text):
        for sentence in paragraph2sentences(paragraph):
            for clause in sentence2clauses(sentence):
                for word in clause2words(clause):
                    stem = word2stem(word)
                    yield word, stem
