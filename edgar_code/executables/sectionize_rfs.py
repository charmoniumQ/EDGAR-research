import itertools
import random
from tqdm import tqdm
from edgar_code.util import new_directory, sanitize_fname, unused_fname, find_file, BOX_PATH
from edgar_code.util.cache import Cache, IndexInFile, NoStore
from edgar_code.retrieve import index_to_rf, download_indexes_lazy
from edgar_code.retrieve.form import index_to_url
from edgar_code.tokenize import text2sections_and_counts


@Cache(IndexInFile('results/cache'), NoStore())
def indexes_for(year):
    indexes = [download_indexes_lazy('10-K', year, qtr) for qtr in tqdm([1,2,3,4], desc='downloading quarter')]
    return list(tqdm(itertools.chain.from_iterable(indexes), desc='parsing index', total=1000))


stop_words = set()
with find_file('stop_words.csv', BOX_PATH).open() as f:
    for stop_word in f:
        stop_words.add(stop_word)


def main(year, n, ciks, dir_):
    indexes = indexes_for(year)
    chosen_indexes = random.sample(indexes, n)
    chosen_indexes += [index for index in indexes if index.CIK in ciks]
    output = []
    for index in tqdm(chosen_indexes, desc='parsing rf'):
        fname = sanitize_fname(index.company_name)
        fname = unused_fname(dir_, fname).with_suffix('.html')
        output.append(f'{index.company_name} -> {fname.name}')
        rf = index_to_rf(index)
        if rf:
            with fname.open('w') as f:
                f.write('<html><body>\n')
                f.write('<a href="{url}">Original 10-K</a>'.format(
                    url=index_to_url('10-K', index)
                ))

                for heading_paragraphs, body_paragraphs, stem_counts in \
                    text2sections_and_counts(rf):
                    f.write('<hr />\n')
                    for paragraph in heading_paragraphs:
                        f.write('<h3>\n' + '\n'.join(paragraph) + '</h3>\n')

                    for paragraph in body_paragraphs:
                        f.write('<p>\n' + '\n'.join(paragraph) + '</p>\n')

                    for stop_word in stop_words:
                        del stem_counts[stop_word.strip()]

                    f.write('<table>\n')
                    f.write('<tr>\n')
                    for stem, freq in stem_counts.most_common(20):
                        f.write(f'<td>{stem}</td>')
                    f.write('</tr>\n')
                    f.write('<tr>\n')
                    for stem, freq in stem_counts.most_common(20):
                        f.write(f'<td>{freq}</td>')
                    f.write('</tr>\n')
                    f.write('</table>\n')
                f.write('</body></html>\n')
        else:
            output.append(['BAD 10-k! Tell sam the year, qtr, and name.'])
    for output_line in output:
        print(output_line)


if __name__ == '__main__':
    year = 2012
    dir_ = new_directory()
    print('results in', dir_)
    main(year, 1, [], dir_)
