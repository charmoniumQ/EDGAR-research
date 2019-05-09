# TODO: investigate the performance implications fo pypy as opposed to miniconda, cpython, or alpine-pypy
FROM pypy:3.6-slim
RUN ln -s /usr/local/bin/pypy3 /usr/local/bin/python3
#FROM python:3.7-slim

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN apt-get -y update && apt-get -y install gcc g++ gfortran pkg-config libopenblas-dev libfreetype6-dev libpng-dev unzip

# this could be templated
# RUN pip3 install {contents of ${PROJECT_ROOT}_code/requirements.txt}

# or it could be a pre-build step
# $ cp ${PROJECT_ROOT}_code/requirements.txt ./
# but then I would want to use hash instead of modtime to cache docker
# ADD requirements.txt /app/requirements.txt
# RUN pip3 install -r /app/requirements.txt

# For now it is verbatim copied from ${PROJECT_ROOT}/requirements.txt
RUN pip3 install typing_extensions \
	dask[bag] \
	google-cloud-storage \
	requests \
	toolz \
	cloudpickle \
	nltk \
	numpy \
	scipy \
	sklearn \
	gensim \
	memory_profiler \
	matplotlib \
	PyQt5 \
	tqdm \
	Pyro4 \ # needed to deserialize the gensim data
	spacy \
	en_core_web_sm \
	chardet \
	msgpack

# custom verison of distrubted
RUN pip3 install --upgrade --no-deps --force-reinstall https://github.com/charmoniumQ/distributed/archive/v0.0.2.zip

# custom version of gensim
RUN pip3 install https://github.com/charmoniumQ/gensim/archive/develop2.zip

# download nltk data
RUN python3 -c "import nltk; nltk.download('stopwords'); nltk.download('punkt')"
