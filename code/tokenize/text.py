import re
non_letter = re.compile('[^a-zA-Z]')


def is_toc(alpha_line):
    return ('tableofcontents' in alpha_line
            # and not much else is on the line
            # (the word 'page' could be on the line)
            and len(alpha_line) <= len('tableofcontents') + 4)


def is_text_line(line):
    # remove non-alphabetic characters
    alpha_line = re.sub(non_letter, '', line).lower()
    # TODO: examine bullet-points in 1-800-FLOWERS
    return len(alpha_line) > 50 and not(is_toc(alpha_line))


def to_paragraphs(lines):
    '''Returns a list of lists of sentences.

    In this project, the input lines were each their own paragraph.'''
    return filter(is_text_line, lines.split('\n'))


__all__ = ['to_paragraphs']
