#!/bin/bash -e

docker push lsstdax/oracle-db-demo:latest
docker push lsstdax/postgresql-db-demo:latest
docker push lsstdax/tap-schema-db:latest
docker push lsstdax/mock-qserv:latest
docker push lsstdax/lsst-tap-demo:latest
docker push lsstdax/presto:latest
