#!/bin/bash -e

# Build the TAP service and docker images.
gradle --stacktrace --info clean assemble javadoc build test
cp build/libs/*.war docker
(cd docker && docker build . -t lsstdax/lsst-tap-service:dev -f Dockerfile.lsst-tap-service)
(cd docker && docker build . -t lsstdax/uws-db:dev -f Dockerfile.uws-db)
(cd docker && docker build . -t lsstdax/mock-qserv:dev -f Dockerfile.mock-qserv)
docker build . -t lsstdax/querymonkey:dev -f docker/Dockerfile.querymonkey

# Build an example TAP_SCHEMA database that is empty other
# than required schema.
(cd tap-schema && docker build . -t lsstdax/tap-schema-db:dev -f Dockerfile)
