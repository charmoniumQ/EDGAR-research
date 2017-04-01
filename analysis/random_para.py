import csv
import random
import os.path
from mining.retrieve_index import get_index
from mining.retrieve_10k import get_risk_factors
from analysis.new_directory import new_directory

def is_paragraph(paragraph):
    return len(paragraph) > 20

def is_hacking(paragraph):
    paragraph = paragraph.lower()
    return 'information security' in paragraph

directory = new_directory()
i = 0
with open(os.path.join(directory, 'paragraphs.csv'), 'w') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(['number', 'text'])
    def write(paragraph):
        global i
        writer.writerow([i, paragraph.strip()])
        i += 1
        print(i)
        f.flush()


    for record in get_index(2016, 3, enable_cache=True, verbose=False, debug=False):
        if random.random() < 0.2:
            rf = get_risk_factors(record['Filename'], enable_cache=False, verbose=False, debug=False, throw=False)
            if rf:
                paragraphs = list(filter(is_paragraph, rf.split('\n')))
                write(random.choice(paragraphs))
        elif random.random() < 0.5:
            rf = get_risk_factors(record['Filename'], enable_cache=False, verbose=False, debug=False, throw=False)
            if rf:
                paragraphs = list(filter(is_paragraph, rf.split('\n')))
                rel_paragraphs = list(filter(is_hacking, paragraphs))
                if rel_paragraphs:
                    write(random.choice(rel_paragraphs))
                    if len(rel_paragraphs) > 1:
                        write(random.choice(rel_paragraphs))
                    write(random.choice(paragraphs))
                    write(random.choice(paragraphs))
        if i > 10:
            break
