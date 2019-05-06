import re
import spacy
nlp = spacy.load('en_core_web_sm')
import nltk
from nltk.util import ngrams
import collections

def is_paragraph(line):
    return sum(ch.isalpha() for ch in line) > 100

def text2paragraphs(text):
    return list(filter(is_paragraph, text.split('\n')))
