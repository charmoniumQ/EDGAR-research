from mining.cache import download
from mining.retrieve_10k import SGML_to_files, extract_to_disk, html_to_text, clean_text

def get_8k(path,enable_cache,verbose,debug):
    sgml = download(path,enable_cache,verbose,debug)
    files = SGML_to_files(sgml, verbose, debug)
    item = parse_8k(path,enable_cache, verbose, debug, path)
    return item


def parse_8k(files, enable_cache,verbose, debug, path):
    # files is a list of dictionary which has 'type' and 'text' as items in the dictionary
    for file in files:
        if file['type'].lower() == "8-k":
            break
        
    html = file['text']
    text = html_to_text(html, verbose, debug, path)
    text = clean_text(text, verbose, False, '')
        
    
    return ""


def extract_8k(directory, path,enable_cache,verbose,debug):
    sgml = download(path,enable_cache,verbose,debug)
    files = SGML_to_files(sgml, verbose, debug)
    extract_to_disk(directory, files, verbose, debug)
    
extract_8k("test_dir6", "edgar/data/727510/0001144204-16-083875.txt", False, True, True)