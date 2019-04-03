import re
boundary = [';', ',', '- ', ':']
boundary_re = re.compile('|'.join(boundary))
non_letter = re.compile('[^a-zA-Z]')


def is_text_phrase(phrase):
    return len(re.sub(non_letter, '', phrase)) >= 1


def sentence2clauses(sentence):
    return list(filter(is_text_phrase, boundary_re.split(sentence)))
