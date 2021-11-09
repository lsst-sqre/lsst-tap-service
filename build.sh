#!/bin/bash -ex

# Build the TAP service and docker images.
gradle --stacktrace --info clean assemble javadoc build test
cp build/libs/*.war docker

# Apply some patches and upstream dependencies that
# haven't been accepted yet.
jar -uf docker/tap##1000.war -C docker/patches WEB-INF/lib/cadc-adql-1.1.10.jar
jar -uf docker/tap##1000.war -C docker/patches WEB-INF/lib/cadc-dali-1.2.11.jar

(cd docker && docker build . -t lsstdax/lsst-tap-service:dev -f Dockerfile.lsst-tap-service)
(cd docker && docker build . -t lsstdax/uws-db:dev -f Dockerfile.uws-db)
(cd docker && docker build . -t lsstdax/mock-qserv:dev -f Dockerfile.mock-qserv)
