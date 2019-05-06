# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
FROM pypy:3.6-slim
RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++ gfortran pkg-config libopenblas-dev libfreetype6-dev libpng-dev

RUN pip3 install https://github.com/charmoniumQ/distributed/archive/issue-1819.zip

# this could be templated
# RUN pip3 install {contents of ${PROJECT_ROOT}/edgar_code/requirements.txt}

# or it could be a pre-build step
# $ cp ${PROJECT_ROOT}/edgar_code/requirements.txt ./
# but then I would want to use hash instead of modtime to cache docker
# ADD requirements.txt /app/requirements.txt
# RUN pip3 install -r /app/requirements.txt

# For now it is verbatim copied from ${PROJECT_ROOT}/edgar_code/requirements.txt
RUN pip3 install dask[bag] google-cloud-storage requests toolz cloudpickle nltk numpy
RUN pip3 install scipy sklearn matplotlib Pyro4
RUN python3 -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"
RUN pip3 install https://github.com/charmoniumQ/gensim/archive/develop2.zip
RUN apt-get install -y unzip
RUN pip3 install --upgrade --no-deps --force-reinstall https://github.com/charmoniumQ/distributed/archive/v0.0.2.zip
RUN pip3 install tqdm
RUN pip3 install memory-profiler
RUN pip3 install spacy
RUN python3 -m spacy download en_core_web_lg
