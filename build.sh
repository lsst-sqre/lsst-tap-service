#!/bin/bash -ex

# Build the TAP service and docker images.
gradle --stacktrace --info clean assemble javadoc build test

cp build/libs/*.war docker

docker build . -t lsstdax/lsst-tap-service:dev -f docker/Dockerfile.lsst-tap-service
docker build . -t lsstdax/uws-db:dev -f docker/Dockerfile.uws-db
docker build . -t lsstdax/mock-qserv:dev -f docker/Dockerfile.mock-qserv
