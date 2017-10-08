from code.retrieve import download_index, download_10k, download_8k


if __name__ == '__main__':
    index = download_index(2014, 1, '10-K')
    for record, rf in download_all(2014, 1, '10-K', 'Item 1A').take(2):
        print(rf[:100])
