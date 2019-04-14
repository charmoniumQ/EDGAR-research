# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
FROM pypy:3.6-slim
RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc

RUN pip3 install https://github.com/charmoniumQ/distributed/archive/issue-1819.zip

# this could be templated
# RUN pip3 install {contents of ${PROJECT_ROOT}/edgar_code/requirements.txt}

# or it could be a pre-build step
# $ cp ${PROJECT_ROOT}/edgar_code/requirements.txt ./
# but then I would want to use hash instead of modtime to cache docker
# ADD requirements.txt /app/requirements.txt
# RUN pip3 install -r /app/requirements.txt

# For now it is verbatim copied from ${PROJECT_ROOT}/edgar_code/requirements.txt
RUN pip3 install dask[bag] google-cloud-storage requests toolz cloudpickle nltk
RUN python3 -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"
