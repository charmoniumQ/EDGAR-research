#!/bin/bash

main="test.py"

python3 setup.py bdist_egg || exit 1

sudo easy_install3 dist/EDGAR_research-0.1-py3.4.egg || exit 1

export PYSPARK_PYTHON=python3

./cluster_config/$main
