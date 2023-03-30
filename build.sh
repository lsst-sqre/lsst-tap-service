#!/bin/bash -ex

# Build the TAP service and docker images.
gradle --stacktrace --info clean assemble javadoc build test
cp build/libs/*.war docker

# Apply some patches and upstream dependencies that
# haven't been accepted yet.
jar -uf docker/tap##1000.war -C docker/patches WEB-INF/lib/cadc-adql-1.1.13.jar

docker build . -t lsstdax/lsst-tap-service:dev -f docker/Dockerfile.lsst-tap-service
docker build . -t lsstdax/uws-db:dev -f docker/Dockerfile.uws-db
docker build . -t lsstdax/mock-qserv:dev -f docker/Dockerfile.mock-qserv
