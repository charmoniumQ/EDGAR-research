# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
# FROM pypy:3.6-slim
# RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3
FROM python:3.7-slim

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++ expect libjpeg-dev zlib1g-dev git-core

# custom version of distributed
RUN pip3 install git+https://github.com/charmoniumQ/distributed@develop2 bokeh Pyro4

# Commands below this line are hotfixes that need to update something without invalidating the cache
# Later they will be incorporated into the commands above it.

RUN pip3 install --upgrade git+https://github.com/charmoniumQ/distributed@develop2
