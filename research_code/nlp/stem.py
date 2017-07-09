import re
import nltk
from nltk.stem import WordNetLemmatizer, porter, lancaster
from nltk.corpus import wordnet
non_lowrcase_letter = re.compile('[^a-z]')


def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''


def lemmatize(words):
    wordnet_lemmatizer = WordNetLemmatizer()
    for word, part_of_speech_ in nltk.pos_tag(words):
        part_of_speech = get_wordnet_pos(part_of_speech_)
        if part_of_speech:
            word = word.lower()
            word = re.sub(non_lowrcase_letter, '', word)
            lemma = wordnet_lemmatizer.lemmatize(word, pos=part_of_speech)
            yield lemma


def porter_stemmer(words):
    stemmer = porter.PorterStemmer()
    return map(stemmer.stem, words)


def lancaster_stemmer(words):
    stemmer = lancaster.LancasterStemmer()
    return map(stemmer.stem, words)


def stem_list(text):
    stemmer = porter.PorterStemmer()
    words = nltk.word_tokenize(text)
    return map(stemmer.stem, words)


def stem(text):
    stemmer = porter.PorterStemmer()
    words = nltk.word_tokenize(text)
    return ' '.join(map(stemmer.stem, words))


def regularize(text):
    text = text.lower()
    return re.sub(non_lowrcase_letter, '', text)


if __name__ == '__main__':
    from tabulate import tabulate
    words = 'financial finances financially - finishing - fin - fine - refusal refuse refusing - referral - referring - referential reference'.split(' ')
    table = [words]
    # table.append(list(lemmatize(words)))
    table.append(list(porter_stemmer(words)))
    table.append(list(lancaster_stemmer(words)))
    print(tabulate(table, tablefmt='plain'))

