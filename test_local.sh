#!/usr/bin/env zsh


python3 -m pylint edgar_code $(find tests -name 'test_*.py' -printf '%p ')
export MYPYPATH=./stubs
python3 -m mypy --strict -p edgar_code
python3 -m mypy --strict $(find tests -name 'test_*.py' -printf '%p ')
python3 -m pytest -v # -m 'not slow'
loc edgar_code tests
