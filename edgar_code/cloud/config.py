import yaml
from edgar_code.util import find_file, BOX_PATH


credentials_file = find_file('credentials.yaml', BOX_PATH)
with credentials_file.open() as f:
    credentials = yaml.load(f)

config_file = credentials_file.parent / 'config.yaml'
if config_file.exists():
    with config_file.open() as f:
        config = yaml.load(f)
else:
    config = {}

def write_config():
    with config_file.open('w+') as f:
        yaml.dump(config, f)
    
