from pathlib import Path
import os
from edgar_code.gs_path import GSPath


cred_path = Path('./service_account.json')
if cred_path.exists():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(cred_path)

cache_path = GSPath.from_url('gs://results-7/cache')

results_path = GSPath.from_url('gs://results-7/results')

cache = True
