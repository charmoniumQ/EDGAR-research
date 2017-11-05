import re
non_letter = re.compile('[^a-zA-Z]')


def text2paragraphs(text):
    '''Returns a list of lists of sentences.

    In this project, the input lines were each their own paragraph.'''
    return filter(is_text_line, text.split('\n'))


def is_toc(alpha_line):
    '''Is it a table of contents line?'''
    return ('tableofcontents' in alpha_line
            # and not much else is on the line
            # (the word 'page' could be on the line)
            and len(alpha_line) <= len('tableofcontents') + 4)


def is_text_line(line):
    '''Is it a text line?'''
    # remove non-alphabetic characters
    alpha_line = re.sub(non_letter, '', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 10 and not(is_toc(alpha_line))
