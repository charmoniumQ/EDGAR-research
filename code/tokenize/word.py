from nltk.stem import porter
stemmer = porter.PorterStemmer()


def to_stem(word):
    return stemmer.stem(word)
