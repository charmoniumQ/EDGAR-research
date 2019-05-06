#!/usr/bin/env zsh

pylint edgar_code
export MYPATH=edgar_code/type_stubs
python3 -m mypy -p edgar_code
export GOOGLE_APPLICATION_CREDENTIALS=edgar_deploy/main-722.service_account.json
py.test
