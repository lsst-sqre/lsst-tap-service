#!/bin/bash

# Build the WAR file that will be installed in the Docker image.

set -exo pipefail

# Build the TAP service and docker images.
gradle --stacktrace --info clean assemble javadoc build test
cp build/libs/*.war docker

# Apply some patches and upstream dependencies that haven't been accepted yet.
jar -uf docker/tap##1000.war -C docker/patches WEB-INF/lib/cadc-adql-1.1.10.jar
