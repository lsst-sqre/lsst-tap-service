#!/bin/bash -ex
cd docker
docker-compose up -d
./waitForContainersReady.sh
./checkAvailability.sh
