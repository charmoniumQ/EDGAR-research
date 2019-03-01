# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
FROM pypy:3-slim
RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++

RUN pip3 install dask[distributed] dask[bag] google-cloud-pubsub

ADD wait_for_scheduler.py /work/wait_for_scheduler.py
ADD test.py /work/test.py
