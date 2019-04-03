import subprocess
from .config import config
from .utils import time_code_decor

@time_code_decor(print_start=False)
def prepare_egg(blob):
    subprocess.run(
        ['python3', 'edgar_code/setup.py', 'bdist_egg'],
        capture_output=True,
    )
    egg = config.project_dir / 'dist/edgar_code-0.1-py3.7.egg'
    blob.upload_from_filename(str(egg))
