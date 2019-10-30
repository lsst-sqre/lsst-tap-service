#!/bin/bash -e
TAG=${1:?'You must specify the image tag to push'}

if [ $TAG == "dev" ]; then
  echo "Don't try to push the dev tag.  Dev is only for local testing."
  exit 1
fi

docker tag lsstdax/lsst-tap-service:dev lsstdax/lsst-tap-service:$TAG
docker tag lsstdax/uws-db:dev lsstdax/uws-db:$TAG
docker tag lsstdax/tap-schema-db:dev lsstdax/tap-schema-db:$TAG
docker tag lsstdax/mock-qserv:dev lsstdax/mock-qserv:$TAG
docker tag lsstdax/querymonkey:dev lsstdax/querymonkey:$TAG

docker push lsstdax/lsst-tap-service:$TAG
docker push lsstdax/uws-db:$TAG
docker push lsstdax/tap-schema-db:$TAG
docker push lsstdax/mock-qserv:$TAG
docker push lsstdax/querymonkey:$TAG
