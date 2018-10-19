#!/bin/bash -e

gradle --info clean build
cp build/libs/tap##1000.war docker
(cd docker && docker build . -t lsstdax/lsst-tap-demo -f Dockerfile)
(cd docker && docker build . -t lsstdax/oracle-db-demo -f Dockerfile.oracle)
(cd docker && docker build . -t lsstdax/postgresql-db-demo -f Dockerfile.postgresql)
