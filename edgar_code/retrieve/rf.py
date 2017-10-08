import toolz
from .form import indexes_to_forms


@toolz.curry
def form_10k_to_rf(form_10k, throw=False):
    if 'Item 1A' in form_10k:
        return form_10k['Item 1A']
    else:
        if throw:
            raise ValueError('The given 10-K has no risk factor')
        return None


@toolz.curry
def indexes_to_rfs(indexes, remove_bad=True, debug_dir=None):
    print(3, debug_dir)
    ret = (
        indexes_to_forms('10-K', indexes, debug_dir=debug_dir)
        .map_values(form_10k_to_rf(throw=False))
    )
    if remove_bad:
        return ret.filter_values(bool)
    else:
        return ret
