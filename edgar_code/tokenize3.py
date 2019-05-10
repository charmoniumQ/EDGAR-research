import collections
import re
import spacy
import nltk
from nltk.util import ngrams


nlp = spacy.load('en_core_web_sm')


def is_paragraph(line):
    return sum(ch.isalpha() for ch in line) > 100


def text2paragraphs(text):
    return list(filter(is_paragraph, text.split('\n')))
