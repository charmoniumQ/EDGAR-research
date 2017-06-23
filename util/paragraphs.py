from util.supress import supress
from nltk.tokenize.punkt import PunktSentenceTokenizer
import re

def is_toc(alpha_line):
    return ('tableofcontents' in alpha_line
            # and not much else is on the line (the word 'page' could be on the line)
            and len(alpha_line) <= len('tableofcontents') + 4)

def is_text_line(line):
    # remove non-alphabetic characters
    alpha_line = re.sub('[^a-zA-Z]', '', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 3 and not(is_toc(alpha_line))

def anounce(it, msg=None):
    for e in it:
        if msg is not None: print(msg, e)
        yield e

def to_paragraphs(text):
    '''Returns a list of paragraphs where each paragraph is a list of sentences.'''
    # http://www.nltk.org/api/nltk.tokenize.html#module-nltk.tokenize.punkt
    with supress():
        punkt = PunktSentenceTokenizer()
        punkt.train(text, verbose=False)

        lines = text.split('\n')
        text_lines = filter(is_text_line, lines)
        paragraphs = map(punkt.tokenize, text_lines)
        return list(paragraphs)
        #return paragraphs

def p_paragraphs(paragraphs, f):
    for paragraph in paragraphs:
        print('  P:', file=f)
        for sentence in paragraph:
            print('    S:', sentence, file=f)

def is_heading(paragraph):
    return len(paragraph) == 1

def get_heading(paragraph):
    return paragraph[0]

def group_paragraphs1(paragraphs):
    heading = None
    body = []
    for paragraph in paragraphs:
        if is_heading(paragraph):
            yield (heading, body)
            heading = get_heading(paragraph)
            body = []
        else:
            body.append(paragraph)
    yield (heading, body)

def group_paragraphs2(paragraphs):
    heading = None
    body = []
    for paragraph in paragraphs:
        yield (paragraph[0], [paragraph[1:]])

def group_paragraphs(paragraphs):
    if len(list(filter(lambda paragraph: len(paragraph) == 1, paragraphs))) > 4:
        return group_paragraphs1(paragraphs)
    else:
        return group_paragraphs2(paragraphs)

def p_groups(groups, f):
    for heading, body in groups:
        if not heading:
            heading = '<No heading detected>'
        print('H:', heading, file=f)
        p_paragraphs(body, f)
        print('\n', file=f)
