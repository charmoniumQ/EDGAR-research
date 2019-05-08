#!/usr/bin/env zsh

# python3 -m pylint edgar_code
export MYPYPATH=./stubs
python3 -m mypy -p edgar_code
python3 -m mypy $(find tests -name 'test_*.py' -printf '%p ')
# python3 -m pytest
