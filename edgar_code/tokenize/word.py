from nltk.stem import porter
stemmer = porter.PorterStemmer()


def word2stem(word):
    return stemmer.stem(word)
