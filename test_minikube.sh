#!/bin/bash -ex
if [ -f dev-chart.tgz ]
then
  CHART=dev-chart.tgz
else
  CHART=lsst-sqre/cadc-tap
fi

helm delete tap-dev -n tap-dev || true
./build.sh
helm upgrade --install tap-dev $CHART --create-namespace --namespace tap-dev --values dev-values.yaml
