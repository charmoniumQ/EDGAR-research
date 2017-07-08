from util.flatten import flatten
from util.supress import supress
from nltk.tokenize.punkt import PunktSentenceTokenizer
import re


non_letter = re.compile('[^a-zA-Z]')


def is_toc(alpha_line):
    return ('tableofcontents' in alpha_line
            # and not much else is on the line
            # (the word 'page' could be on the line)
            and len(alpha_line) <= len('tableofcontents') + 4)


def is_text_line(line):
    # remove non-alphabetic characters
    alpha_line = re.sub(non_letter, '', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 50 and not(is_toc(alpha_line))


def to_paragraphs(text):
    '''Returns a list of lists of sentences.'''
    # http://www.nltk.org/api/nltk.tokenize.html#module-nltk.tokenize.punkt
    with supress():

        lines = text.split('\n')
        text_lines = list(filter(is_text_line, lines))

        if text_lines:
            punkt = PunktSentenceTokenizer()
            punkt.train(text, verbose=False)
            paragraphs = list(map(punkt.tokenize, text_lines))
            return paragraphs
        else:
            return []


def to_sentences(text):
    '''returns a list of sentences'''
    paragraphs = to_paragraphs(text)
    return flatten(paragraphs)


def is_text_phrase(phrase):
    return len(re.sub(non_letter, '', phrase)) >= 1


boundary = [';', ',', '- ', ':']
boundary_re = re.compile('|'.join(boundary))


def to_clauses(sentence):
    return list(filter(is_text_phrase, boundary_re.split(sentence)))


def is_heading(paragraph):
    return len(paragraph) == 1


def get_heading(paragraph):
    return paragraph[0]


def group_paragraphs1(paragraphs):
    '''Assume paragraphs with one sentence are headers'''
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
    '''Assume the first sentence of each paragraph is the header'''
    for paragraph in paragraphs:
        yield (paragraph[0], [paragraph[1:]])


def group_paragraphs(paragraphs):
    if len(list(filter(lambda paragraph: len(paragraph) == 1, paragraphs))) > 4:
        return group_paragraphs1(paragraphs)
    else:
        return group_paragraphs2(paragraphs)


def p_paragraphs(paragraphs, f):
    for paragraph in paragraphs:
        print('  P:', file=f)
        for sentence in paragraph:
            print('    S:', sentence, file=f)


def p_groups(groups, f):
    for heading, body in groups:
        if not heading:
            heading = '<No heading detected>'
        print('H:', heading, file=f)
        p_paragraphs(body, f)
        print('\n', file=f)
