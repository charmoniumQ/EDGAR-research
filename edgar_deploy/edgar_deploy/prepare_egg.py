import subprocess
from .config import config
from .gs_path import copy
from .time_code import time_code


@time_code.decor(print_start=False)
def prepare_egg(gs_egg_path):
    subprocess.run(
        ['python3', 'setup.py', 'bdist_egg'],
        capture_output=True,
        cwd=str(config.project_dir),
    )
    egg_path = config.project_dir / 'dist/edgar_code-0.1-py3.7.egg'
    copy(egg_path, gs_egg_path)
    return gs_egg_path.public_path()
