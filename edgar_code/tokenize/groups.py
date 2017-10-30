def is_heading1(paragraph):
    return len(paragraph) == 1


def get_heading1(paragraph):
    return paragraph[0]


def to_groups1(paragraphs):
    '''Assume paragraphs with one sentence are headers'''
    heading = None
    body = []
    for paragraph in paragraphs:
        if is_heading1(paragraph):
            yield (heading, body)
            heading = get_heading1(paragraph)
            body = []
        else:
            body.append(paragraph)
    yield (heading, body)


def to_groups2(paragraphs):
    '''Assume the first sentence of each paragraph is the header'''
    for paragraph in paragraphs:
        yield (paragraph[0], [paragraph[1:]])


def to_groups(paragraphs):
    # assuming each new line is its own heading
    # is there a reasonable number of headings?
    if len(list(filter(is_heading1, paragraphs))) > 4:
        return to_groups1(paragraphs)
    else:
        # if not, then assume the first sentence is a heading
        return to_groups2(paragraphs)


__all__ = ['to_groups']