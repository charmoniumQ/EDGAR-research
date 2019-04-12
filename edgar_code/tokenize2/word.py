import re
import nltk.stem.porter
import nltk.corpus
stemmer = nltk.stem.porter.PorterStemmer()


def word2stem(word):
    return stemmer.stem(word)


word_regex = re.compile('^[a-z]*$', re.IGNORE_CASE)
def is_word(word):
    match = word_regex.match(word)
    if match:
        return True
    else:
        return False


stop_words = set(nltk.corpus.stopwords.words('english'))
def is_significant_word(word):
    return word not in stop_words and len(word) >= 5
