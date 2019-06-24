
from typing import List, Dict, Iterable, Union
import collections
import csv
from typing_extensions import Counter
import edgar_code.cli.config as config
from edgar_code.gs_path import PathLike
from edgar_code.util import time_code, flatten_iter
from edgar_code.retrieve import get_rfs
from edgar_code.tokenize3 import paragraph2terms, TermsType, PhraseCount
from edgar_code.types import Bag


@config.cache_decor
def get_terms(year: int, qtr: int) -> Bag[List[TermsType]]:

    def ensure_str(arg: Union[List[str], Exception]) -> List[str]:
        if isinstance(arg, list):
            return arg
        else:
            raise RuntimeError()

    return (
        get_rfs(year, qtr)
        .filter(lambda rf: not isinstance(rf, Exception))
        .map(ensure_str) # TODO: eliminate; this is for mypy
        .map(lambda paragraphs: list(map(paragraph2terms, paragraphs)))
    )

def aggregate_terms(
        entities_ngrams_stems: Iterable[TermsType]
) -> TermsType:
    all_entities: PhraseCount = Counter()
    all_ngrams: Dict[int, PhraseCount] = collections.defaultdict(Counter)
    all_stem2word: Dict[str, Counter[str]] = collections.defaultdict(Counter)
    for entity, ngram_counts, stem2word in entities_ngrams_stems:
        all_entities += entity
        for n, ngram_counter in ngram_counts.items():
            all_ngrams[n] += ngram_counter
        for stem, word_counter in stem2word.items():
            all_stem2word[stem] += word_counter
    return all_entities, all_ngrams, all_stem2word

def get_combined_terms(year: int, qtr: int) -> TermsType:
    return (
        get_terms(year, qtr)
        .map(aggregate_terms)
        .reduction(aggregate_terms, aggregate_terms)
    )

def get_alltime_terms() -> TermsType:
    client = config.get_client()
    futures_terms = [
        client.compute(get_combined_terms(year, qtr), sync=False)
        for year in range(1995, 2019)
        for qtr in range(1, 5)
    ]
    return aggregate_terms(
        # aggregate computation one at a time in this generator,
        # freeing workers to do other tasks
        future_terms.result() for future_terms in futures_terms
    )


def main() -> None:
    with time_code.ctx(f'aggregate terms'):
        entities, ngram_counters, stem2words = get_alltime_terms()

    with time_code.ctx('output terms'):
        terms_dir = config.results_path / 'get_terms'

        # I'll write this once and reuse it.
        def write_phrase_counts(
                path: PathLike, phrase_counts: PhraseCount
        ) -> None:
            with path.open('w') as fil:
                csv.writer(fil).writerows([
                    (str(count),) + phrase
                    for phrase, count in phrase_counts.most_common()
                ])

        write_phrase_counts(terms_dir / 'entities.csv', entities)

        for n, ngram_counter in ngram_counters.items():
            write_phrase_counts(terms_dir / f'{n}_grams.csv', ngram_counter)

        with (terms_dir / 'stems.csv').open('w') as entities_file:
            entities_csv = csv.writer(entities_file)
            for stem, word_counter in stem2words.items():
                entities_csv.writerow(
                    [stem] + list(flatten_iter(map(str, word_counter.most_common(10))))
                )


if __name__ == '__main__':
    main()
