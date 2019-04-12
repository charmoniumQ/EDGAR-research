# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
FROM pypy:3.6-slim
RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++

RUN pip3 install dask[bag] google-cloud-storage https://github.com/charmoniumQ/distributed/archive/issue-1819.zip

ADD wait_for_scheduler.py /work/wait_for_scheduler.py
ADD runner.py /work/runner.py

# see worker/Dockerfile
RUN pip3 install dask[bag] google-cloud-storage requests toolz cloudpickle nltk
RUN python3 -c "import nltk; nltk.download('stopwords')"
