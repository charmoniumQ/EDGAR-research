#!/bin/bash

main="test.py"

python2 setup.py bdist_egg
sudo easy_install dist/EDGAR_research-0.1-py2.7.egg
./cluster_config/$main
