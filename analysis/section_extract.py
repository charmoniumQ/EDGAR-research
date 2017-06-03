from __future__ import print_function
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors
from util.new_directory import new_directory

'''
This script downloads and parses risk factors
year: (int)
qtr: (int) 1-4
'''

year = 2016
qtr = 3

directory = new_directory()
print('Results in ' + directory.as_posix())

# write meta-data
with (directory / 'info.txt').open('w+') as f:
    f.write('''script: {__file__}
year: {year}
qtr: {qtr}
'''.format(**locals()))

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

def regularize(text):
    wordnet_lemmatizer = WordNetLemmatizer()
    new_text = []
    for word, part_of_speech_ in nltk.pos_tag(nltk.word_tokenize(text)):
        part_of_speech = get_wordnet_pos(part_of_speech_)
        if part_of_speech:
            word = word.lower()
            word = re.sub('[^a-z]', '', word)
            lemma = wordnet_lemmatizer.lemmatize(word, pos=part_of_speech)
            new_text.append(lemma)
    return ' '.join(new_text)

#for record in get_index(year, qtr, enable_cache=True):
import itertools
for record in itertools.islice(get_index(year, qtr, enable_cache=True), None, 100):
    name = record['Company Name']
    # TODO: lint
    # TODO: move this to new_directory
    import re
    name = re.sub('[^a-zA-Z0-9]', '_', name)
    file_name = '{name}.txt'.format(**locals())
    rf = get_risk_factors(record['Filename'], enable_cache=True, throw=False)
    if len(rf) > 1000:
        print(record['Company Name'])
        with (directory / file_name).open('w+', encoding='utf-8') as f:
            f.write(regularize(rf))
