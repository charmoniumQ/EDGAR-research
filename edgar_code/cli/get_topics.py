from typing import List, Tuple
import csv
import edgar_code.cli.config as config
# from edgar_code.tokenize3 import TermsType
# from edgar_code.types import Bag
# from edgar_code.cli.get_terms import get_terms


def get_sorted_terms() -> List[Tuple[str, ...]]:
    terms_path = config.results_path / 'get_topics/terms.csv'
    with terms_path.open() as terms_file:
        terms = [tuple(row[1:]) for row in csv.reader(terms_file)]
    return sorted(terms)


# def get_bows(year: int, qtr: int) -> Bag[List[Tuple[int, float]]]:
#     terms = get_sorted_terms()
#     def doc2bow(
#             # terms: List[Tuple[str]],
#             term_types: TermsType,
#     ) -> List[Tuple[int, float]]:
#         _, ns_counts, _ = term_types
#         return [
#             (i, ns_counts[len(term)][term])
#             for i, term in enumerate(terms)
#             if term in ns_counts[len(term)]
#         ]

#     return (
#         get_terms(year, qtr)
#         .map(doc2bow)
#     )
