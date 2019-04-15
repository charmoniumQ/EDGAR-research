import re
import nltk.stem.porter
import nltk.corpus
stemmer = nltk.stem.porter.PorterStemmer()


def normalize(word):
    return word.lower()


def word2stem(word):
    return stemmer.stem(word)


word_regex = re.compile('^[a-z]*$', re.IGNORECASE)
def is_word(word):
    match = word_regex.match(word)
    if match:
        return True
    else:
        return False

stop_words = set(nltk.corpus.stopwords.words('english'))
important_short_words = set('''
trump flu swine obama hack zika
'''.strip().split(' '))
def is_significant_word(word):
    return (
        is_word(word)
        and word not in stop_words
        and (len(word) >= 5 or word in important_short_words)
    )
