from cluster_config.download import Status, get_items


def count(record):
    output = dict(time=record['time']['total'],
                  download_time=record['time']['download'])
    if record['error'] == Status.ERROR:
        return output.update(dict(total=1, valid=0, item=0, size=0))
    elif record['error'] == Status.NOT_FOUND:
        return output.update(dict(total=1, valid=1, item=0, size=0))
    else:
        return output.update(dict(total=1, valid=1, item=1,
                                  size=len(record['item'])))


def dict_add(d1, d2):
    '''Adds two dicts on their common keys'''
    return {key: d1[key] + d2[key] for key in d1.keys() & d2.keys()}


def interpret_count(count):
    total_time = count['time'].total_seconds()
    download_time = count['download'].total_seconds()
    data_throughput = count['size'] / count['time'] / 1e3
    doc_throughput = count['total'] / count['time']

    avg_size = (count['size'] / count['item']) / 1e3
    size = count['size'] / 1e6

    item_ratio = (count['item'] / count['valid']) * 1e2
    valid_ratio = (count['valid'] / count['total']) * 1e2
    return '''
{total_time:.1f} sec of time ({download_time:.1f} sec in download/parse)
{data_throughput:f} kbytes / sec
{doc_throughput:f} docs / sec

{size:.1f} MB in size ({avg_size:.0f} KB / doc)

{valid_ratio:.0f}% ({count[valid]} / {count[total]})
{item_ratio:.0f}% ({count[item]} / {count[valid]})
'''.format(**locals())


def stats_for(year, qtr, item):
    res = (
        get_items(year, qtr, item)
        .map(count)
        .reduce(dict_add)
        .collect()
    )
    return interpret_count(res)


def stats_fors(years, item):
    for year in years:
        for qtr in range(1, 5):
            yield stats_for(year, qtr, item)


if __name__ == '__main__':
    years = range(2007, 2017)
    map(print, stats_fors(years, '1a'))
