import re
non_letter = re.compile('[^a-zA-Z ]')


def regularize(text):
    text = text.lower()
    return re.sub(non_letter, '', text)


def to_words(clause):
    return filter(bool, regularize(clause).split(' '))
