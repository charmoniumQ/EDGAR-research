# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
FROM pypy:3.6-slim
RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++ expect libjpeg-dev zlib1g-dev

RUN pip3 install google-cloud-storage https://github.com/charmoniumQ/distributed/archive/issue-1819.zip bokeh

RUN pip3 install Pyro4

RUN pip3 install --upgrade --no-deps --force-reinstall https://github.com/charmoniumQ/distributed/archive/v0.0.2.zip

ADD update_status.py /app/update_status.py
