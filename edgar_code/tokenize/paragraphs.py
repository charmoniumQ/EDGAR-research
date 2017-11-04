from nltk.tokenize.punkt import PunktSentenceTokenizer


def paragraph2sentences_func(text):
    '''Returns a list of sentences for each paragraph'''
    punkt = PunktSentenceTokenizer(train_text=text)
    def paragraph2sentences(paragraph):
        '''Returns a list of sentences from the paragraph'''
        return punkt.tokenize(paragraph)
    return paragraph2sentences
