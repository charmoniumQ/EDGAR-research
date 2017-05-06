from nltk.tokenize.punkt import PunktSentenceTokenizer
import re

def is_text_line(line):
    return len(line) > 5 and ' ' in line and \
        not(line.lower().contains('table of contents') and len(line) < 20)

# http://www.nltk.org/api/nltk.tokenize.html#module-nltk.tokenize.punkt
punkt = PunktSentenceTokenizer(verbose=False)
def paragraphs(text):
    punkt.train(text, verbose=False)
    for sentence in punkt.tokenize(text):
        lines = [line for line in sentence.split('\n')
                 if is_text_line(line)] # removes page-numbers and page-headers
        sentences = '\n'.join(lines)
        if sentence:
            # guard against sentences which at this point are blank
            yield sentence
