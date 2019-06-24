from typing import List, Tuple
import csv
import dask.delayed
import edgar_code.cli.config as config
from edgar_code.tokenize3 import TermsType
from edgar_code.types import Bag
from edgar_code.cli.get_terms import get_terms
from edgar_code.map_const import map_const


def get_sorted_terms() -> List[Tuple[str, ...]]:
    terms_path = config.results_path / 'get_topics/terms.csv'
    with terms_path.open() as terms_file:
        terms = [tuple(row[1:]) for row in csv.reader(terms_file)]
    return sorted(terms)


@config.cache_decor
def get_bows(year: int, qtr: int) -> Bag[List[List[Tuple[int, float]]]]:
    def doc2bow(
            term_types: TermsType,
            terms: List[Tuple[str]],
    ) -> List[Tuple[int, float]]:
        _, ns_counts, _ = term_types
        return [
            (i, ns_counts[len(term)][term])
            for i, term in enumerate(terms)
            if term in ns_counts[len(term)]
        ]

    def docs2bows(
            term_types_paragraphs: List[TermsType],
            terms: List[Tuple[str]]
    ) -> List[List[Tuple[int, float]]]:
        return [doc2bow(term_types, terms) for term_types in term_types_paragraphs]

    return map_const(
        docs2bows,
        get_terms(year, qtr),
        dask.delayed.delayed(get_sorted_terms)(),
    )


def main() -> None:
    pass
