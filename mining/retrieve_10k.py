from os import mkdir
from os.path import join
import re
from mining.cache import download
from bs4 import BeautifulSoup

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

def html_to_text(textin):
    '''Extract real text from HTML, after removing table of contents'''
    textin = textin.decode()
    newline_tags = 'p div tr'.split(' ')
    newline_tags += [tag.upper() for tag in newline_tags]
    textin = textin.replace('\n', ' ')
    for tag in newline_tags:
        textin = textin.replace('<' + tag, '\n<' + tag)
    html_10k = BeautifulSoup(textin, 'html.parser')
    text = html_10k.text.replace('\xa0', ' ') # &nbsp -> ' '
    text = re.sub('  +', ' ', text)
    try:
        # text = re.sub(r'.*?^ ?part i$.*?^ ?part i *$', '', text, flags=re.MULTILINE | re.DOTALL | re.IGNORECASE)
        start = re.search('^ ?part i[\. \n]', text, re.MULTILINE | re.IGNORECASE).end()
        text = text[start:]
        start = re.search('^ ?part i[\. \n]', text, re.MULTILINE | re.IGNORECASE).end()
        text = text[start:]
    except:
        with open('crap.txt', 'w') as f:
            f.write(text)
        raise RuntimeError('Could not find part 1 (removing table of contents)')
    return text

items = ['Item 1', 'Item 1A', 'Item 1B', 'Item 2', 'Item 3', 'Item 4', 'Item 5', 'Item 6', 'Item 7', 'Item 7A', 'Item 8', 'Item 9', 'Item 9A', 'Item 9B', 'Item 10', 'Item 11', 'Item 12', 'Item 13', 'Item 14', 'Item 15', 'Signatures']
names = '1 1A 1B 2 3 4 5 6 7 7A 8 9 9A 9B 10 11 12 13 14 15'.split(' ')

def parse_10k(files):
    '''Inputs a list of dicts (one dict for each file) aad returns a dict mapping from names (declared above) to strings of content'''
    for file_info in files:
        if file_info['type'] == '10-K':
            break
    else:
        raise RuntimeError('Cannot find the 10K')

    text = html_to_text(file_info['text'])
    # print('Normalized text...')

    contents = {}
    for name, item, next_item in zip(names, items[:-1], items[1:]):
        item_pattern = re.compile(r'^\s*({item}.*?)$(.*?)(?=^\s*{next_item})'.format(**locals()), re.DOTALL | re.MULTILINE | re.IGNORECASE)
        match = item_pattern.search(text)
        if not match:
            with open('crap.txt', 'w') as f:
                f.write(text)
            print('Could not find {item}'.format(**locals()))
        else:
            contents[name] = match.group(2)
            text = text[match.end():]
    return contents


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
