import re
non_letter = re.compile('[^a-zA-Z ]')


def regularize(text):
    """
    Given some text, this method will eliminate non-letter characters
    and convert all letters to lower case.
    :param text: some input text string
    :return: the regularized text string

    For example:
    `regularize('I Got 100')`
    returns
    i got
    """
    text = text.lower()
    return re.sub(non_letter, '', text)


def clause2words(clause):
    """
    Given some text string, this function with regularizes the input through `regularize(clause)`,
    then it will return a list words in that clause.
    :param clause: some input text string
    :return: an iterable of words

    For example:
    `to_words('I Got 100')`
    returns an iterable containing 'i' and 'got'
    """
    return list(filter(bool, regularize(clause).split(' ')))
