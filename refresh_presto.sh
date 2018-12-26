#!/bin/bash -e

rm -rf docker/presto-server-*
cp -r ~/presto/presto-server/target/presto-server-*/ docker/presto-server-SNAPSHOT
cp -r ~/presto/presto-qserv/target/presto-qserv-0.214-SNAPSHOT docker/presto-server-SNAPSHOT/plugin/qserv

(cd docker && docker build . -t lsstdax/presto:dev -f Dockerfile.presto)
docker push lsstdax/presto:dev
