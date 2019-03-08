import logging
import os
import yaml
from pathlib import Path
from haikunator import Haikunator
import tempfile
from . import utils


class Config(object):
    # I am justified in hardcoding instance variables, because this
    # could be replaced with a dynamic property.
    def __init__(self):
        self.module_dir = Path(__loader__.path).parent
        self.project_dir = self.module_dir.parent

        with open(self.module_dir / 'config.yaml', 'r') as f:
            config = yaml.load(f)

        self.gcloud = utils.Struct()
        self.gcloud.project = config['gcloud']['project']
        self.gcloud.fq_zone = f"{config['gcloud']['region']}-{config['gcloud']['zone']}"
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(self.module_dir / config['gcloud']['service_account_file'])

        self.run_name = Haikunator.haikunate(0)

        self.logging_level = getattr(logging, config['logging'].upper())
        logging.basicConfig(level=self.logging_level)

        self._scratch_dir = tempfile.TemporaryDirectory()
        # this will be deleted when this classes deconstructor is
        # called, which happens when the program exits.
        self.scratch_dir = Path(self._scratch_dir.name)

        self.cache_dir = Path(self.project_dir / 'cache')


config = Config()
