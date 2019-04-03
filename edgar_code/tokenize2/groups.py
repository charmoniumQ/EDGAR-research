def is_heading1(paragraph):
    return len(paragraph) == 1


def paragraphs2groups1(paragraphs):
    '''Assume paragraphs with one sentence are headers'''
    heading = []
    body = []
    for paragraph in paragraphs:
        if len(paragraph) == 1:
            if heading or body:
                yield (heading, body)
            heading = [paragraph]
            body.clear()
        else:
            body.append(paragraph)
    yield (heading, body)


def paragraphs2groups2(paragraphs):
    '''Assume the first sentence of each paragraph is the header'''
    for paragraph in paragraphs:
        yield (paragraph[0], [paragraph[1:]])


def paragraphs2groups(paragraphs):
    # assuming each new line is its own heading
    # is there a reasonable number of headings?
    if len(list(filter(is_heading1, paragraphs))) > 4:
        return paragraphs2groups1(paragraphs)
    else:
        # if not, then assume the first sentence is a heading
        return paragraphs2groups2(paragraphs)
