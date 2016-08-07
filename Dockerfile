FROM ywx217/docker-webmanage-base:latest
MAINTAINER Wenxuan Yang "ywx217@gmail.com"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python2.7 \
        python-gevent \
        python-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    redis \
    pymongo==2.8 \
    msgpack-python

WORKDIR /python
COPY ./*.py /python/
COPY ./_queue_data.base64 /python/

