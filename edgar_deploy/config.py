import logging
import sys
import yaml
from pathlib import Path
from haikunator import Haikunator
from google.oauth2.service_account import Credentials
import tempfile


class Struct(object):
    pass


class Config(object):
    # I am justified in hardcoding instance variables, because this
    # could be replaced with a dynamic property.
    def __init__(self):
        self.module_dir = Path(sys.argv[0]).parent

        with open(self.module_dir / 'config.yaml', 'r') as f:
            config = yaml.load(f)

        self.gcloud = Struct()
        self.gcloud.credentials = Credentials.from_service_account_file(
            self.module_dir / config['gcloud']['service_account_file']
        )
        self.gcloud.project = config['gcloud']['project']
        self.gcloud.fq_zone = f"{config['gcloud']['region']}-{config['gcloud']['zone']}"

        self.run_name = Haikunator.haikunate(0)

        self.logging_level = getattr(logging, config['logging'].upper())
        logging.basicConfig(level=self.logging_level)

        self._scratch_dir = tempfile.TemporaryDirectory()
        # this will be deleted when this classes deconstructor is
        # called, which happens when the program exits.
        self.scratch_dir = Path(self._scratch_dir.name)


config = Config()
