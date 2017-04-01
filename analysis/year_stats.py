from mining.retrieve_index import get_index

for year in range(2015, 2017+1):
    for qtr in range(1, 4+1):
        index = get_index(year, qtr, enable_cache=True, verbose=False, debug=True)
        num_docs = len(list(index))
        print('{year:d},{qtr:d},{num_docs:d}'.format(**locals()))
