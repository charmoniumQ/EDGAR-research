# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
# FROM pypy:3.6-slim
# RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3
FROM python:3.7-slim

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++ gfortran pkg-config libopenblas-dev libfreetype6-dev libpng-dev unzip git-core

RUN pip3 install -U pip

# custom verison of distrubted and gensim
RUN pip3 install --upgrade \
    git+https://github.com/charmoniumQ/distributed@develop2 \
    git+https://github.com/charmoniumQ/gensim@develop2 \
    -r https://raw.githubusercontent.com/charmoniumQ/EDGAR-research/master/requirements.txt

# download nltk data
RUN python3 -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"

# download spacy data
RUN python -m spacy download en_core_web_sm

# Commands below this line are hotfixes that need to update something without invalidating the cache
# Later they will be incorporated into the commands above it.

RUN pip3 install --upgrade git+https://github.com/charmoniumQ/distributed@develop2

COPY ./.dask/config.yaml /root/.dask/config.yaml

RUN pip3 install --force-reinstall git+https://github.com/charmoniumQ/distributed@e06c8714dfd91b51cef053f0de04065c1c376121
