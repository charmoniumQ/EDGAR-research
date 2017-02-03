from os import mkdir
from os.path import join
import re
from mining.cache import download
from bs4 import BeautifulSoup

def html_to_text(textin):
    '''Extract real text from HTML, after removing table of contents'''

    textin = textin.decode()

    # replace line breaks within elements with spaces
    textin = textin.replace('\n', ' ')

    # replace line breaks
    newline_tags = 'p div tr P DIV TR'.split(' ')
    for tag in newline_tags:
        textin = textin.replace('<' + tag, '\n<' + tag)

    # parse as HTML and extract text
    html_10k = BeautifulSoup(textin, 'html.parser')
    text = html_10k.text

    # &nbsp -> ' '
    text = text.replace('\xa0', ' ')


    # replace multiple spaces with a single one
    text = re.sub('  +', ' ', text)

    try:
        # remove header
        parti = re.search('^ ?part i[\. \n]', text, re.MULTILINE | re.IGNORECASE)
        text = text[parti.end():]
        
        # remove table of contents
        parti = re.search('^ ?part i[\. \n]', text, re.MULTILINE | re.IGNORECASE)
        if parti:
            text = text[parti.end():]
        else:
            # this means there was no table of contents
            pass
    except:
        with open('10k_error.txt', 'w') as f:
            f.write(text)
        raise RuntimeError('Could not find part 1 (removing table of contents)')
    return text

items = ['Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5', 'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A', 'Item 9B', 'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14', 'Item 15', 'Signatures']
names = '1 1A 1B 2 3 4 5 6 7 7A 8 9 9A 9B 10 11 12 13 14 15'.split(' ')

def text_to_items(text):
    contents = {}
    for name, item, next_item in zip(names, items[:-1], items[1:]):
        item_pattern = re.compile(r'^\s*({item}.*?)$(.*?)(?=^\s*{next_item})'.format(**locals()), re.DOTALL | re.MULTILINE | re.IGNORECASE)
        match = item_pattern.search(text)
        if not match:
            with open('10k_error_{item}.txt'.format(**locals()), 'w') as f:
                f.write(text)
            print('Could not find {item}'.format(**locals()))
        else:
            contents[name] = match.group(2)
            text = text[match.end():]
    return contents

def parse_10k(files):
    '''Inputs a list of dicts (one dict for each file) aad returns a dict mapping from names (declared above) to strings of content'''
    for file_info in files:
        if file_info['type'] == '10-K':
            break
    else:
        raise RuntimeError('Cannot find the 10K')

    text = html_to_text(file_info['text'])
    print('Normalized text...')
    return text_to_items(text)


def extract_to_disk(directory, files):
    '''Extracts all files in the list of dicts (one for each file) into the directory for manual examination'''
    mkdir(directory)
    for file in files:
        if file['type'] == '10-K':
            print(file['filename'])
        try:
            dfname = join(directory, file['filename'])
            with open(dfname, 'wb') as f:
                f.write(file['text'])
        except:
            print(file.keys())

def SGML_to_files(sgml_contents):
    '''Inputs the downloaded SGML and outputs a list of dicts (one dict for each file)

    Each document described in the SGML gets converted to a dict of all of its attributes, including the 'text', which has actual text of a document'''
    doc_pattern = re.compile(b'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL)
    files = []
    for doc_match in doc_pattern.finditer(sgml_contents):
        doc_text = doc_match.group(1)
        files.append({})

        text_pattern = re.compile(b'(.*)<TEXT>(.*?)</TEXT>(.*)', re.DOTALL)
        text_match = text_pattern.search(doc_text)
        files[-1]['text'] = text_match.group(2)
        rest_text = text_match.group(1) + text_match.group(3)

        # Match both forms
        # <TAG>stuff
        # <OTHERTAG>more stuff
        # and
        # <TAG>stuff</TAG>
        # <OTHERTAG>more stuff</OTHERTAG>
        tagcontent_pattern = re.compile(b'<(.*?)>(.*?)[\n<]', re.DOTALL)
        for tagcontent in tagcontent_pattern.finditer(rest_text):
            tagname = tagcontent.group(1).lower().decode()
            content = tagcontent.group(2).decode()
            files[-1][tagname] = content
    return files

def get_risk_factors(path):
    sgml = download(path)
    files = SGML_to_files(sgml.read())
    sgml.close()
    # print('Parsed SGML document')
    # try:
    #     extract_to_disk(path.split('/')[2], files)
    # except:
    #     pass
    risk_factors = parse_10k(files)['1A']
    return risk_factors

if __name__ == '__main__':
    a = download('edgar/data/1382219/0001185185-16-004954.txt')
    files = SGML_to_files(a.read())
    a.close()
    print('Parsed SGML document')
    # extract_to_disk('output3', files)
    b = parse_10k(files)
    print({key: len(val) for key, val in b.items()})
