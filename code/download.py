import re
import itertools
import code.retrieve


def download(years, qtrs, form_type, item=None, filters=[]):
    '''Downloads form_type for years/qtrs and extracts item subject to filters

    form_type (str): "10-k" or "8-k"

    qtrs (list(int) or None): (1-4) or None. None takes alll quarters for that
    year

    filters (list(func)): a list of functions mapping from
    f(record, form, item) -> bool where record is a record in the index, form
    is the parsed form, and item is the requested item in the form. Several
    of such filters are defined below.
    '''

    records = download_records(years, qtrs, form_type, filters)
    return download_doc(records, form_type, item, filters)


def download_records(years, qtrs, form_type, filters):
    if qtrs is None:
        qtrs = range(1, 5)

    for year, qtr in itertools.product(years, qtrs):
        index = code.retrieve.index(year, qtr, form_type)
        for record in index:
            # check predicates
            if all(filter_(record, None, None) for filter_ in filters):
                yield record


def download_doc(records, form_type, item, filters):
    retrieve_form = retrievers[form_type]

    for record in records:
        # form = retrieve_form(record)
        form = None

        if item is None:
            doc = None
        else:
            if item not in form:
                continue  # skip this doc, continue with next
            doc = form[item]

        # check predicates
        if all(filter_(record, form, doc) for filter_ in filters):
            yield record, form, doc


retrievers = {
    '10-k': code.retrieve.f10k,
    '8-k': code.retrieve.f8k,
}


def filter_CIK(CIKs):
    def filter(record, form, item):
        return record['CIK'] in CIKs
    return filter


def filter_name(names):
    names = set(map(lambda x: x.upper(), names))

    def filter(record, form, item):
        return record['company_name'] in names
    return filter


def content_contains(term):
    def filter(record, form, item):
        return term.lower() in item.lower()
    return filter


def content_matches(regex):
    def filter(record, form, item):
        return re.search(regex, item)
    return filter


def min_size(size):
    def filter(record, form, item):
        return len(item) > size


if __name__ == '__main__':
    records = download([2014], None, '10-k', None, [])
    for i, record in zip(range(10), records):
        print(record)
