#!/bin/bash -ex
if [ -f dev-chart.tgz ]
then
  CHART=dev-chart.tgz
else
  CHART=lsst-sqre/cadc-tap
fi

if [ -f tap-schema-dev-chart.tgz ]
then
  TAP_SCHEMA_CHART=tap-schema-dev-chart.tgz
else
  TAP_SCHEMA_CHART=lsst-sqre/tap-schema
fi

helm delete tap-dev -n tap-dev || true
helm delete tap-schema -n tap-schema || true

./build.sh
helm upgrade --install tap-dev $CHART --create-namespace --namespace tap-dev --values dev-values.yaml
helm upgrade --install tap-schema $TAP_SCHEMA_CHART --create-namespace --namespace tap-schema --values tap-schema-values.yaml
