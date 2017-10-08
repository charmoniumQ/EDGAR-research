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


def to_words(clause):
    """
    Given some text string, this function with regularizes the input through `regularize(clause)`,
    then it will return a list words in that clause.
    :param clause: some input text string
    :return: an iterable of words

    For example:
    `to_words('I Got 100')`
    returns an iterable containing 'i' and 'got'
    """
    return filter(bool, regularize(clause).split(' '))


def main():
    example = 'I Got 100'
    print(regularize(example))
    o = to_words(example)
    for i in o:
        print(i)

if __name__ == '__main__':
    main()
