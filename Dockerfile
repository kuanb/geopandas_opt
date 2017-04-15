FROM python:3.5.2

RUN mkdir -p /provisioning
WORKDIR /provisioning

# Install OS dependencies
RUN apt-get update && apt-get install -y \
      build-essential \
      curl \
      libhdf5-dev \
      libgeos-dev \
      unzip && \
    rm -rf /var/lib/apt/lists/*

# Get the spatialindex lib and build
# Needed in order to utilize rtree py package (rtree is a wrapper for it)
RUN mkdir -p /provisioning/spatialindex && \
    cd /provisioning/spatialindex && \
    curl -# -O http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz && \
    tar -xf spatialindex-src-1.8.5.tar.gz && \
    cd spatialindex-src-1.8.5 && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    ldconfig && \
    rm -rf /provisioning/spatialindex

COPY ./requirements.txt /provisioning/requirements.txt

RUN pip install -r ./requirements.txt

COPY . /provisioning
