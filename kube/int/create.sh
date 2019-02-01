#!/bin/bash -ex
# Deploy to the lsst-lsp-int environment.
# This assumes it is being run at NCSA.
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-int'}

# Create the oracle backend for TAP data.
kubectl create -f oracle-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f oracle-service.yaml --namespace $DAX_NAMESPACE

# Create the backend for TAP_SCHEMA data.
kubectl create -f tap-schema-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f tap-schema-service.yaml --namespace $DAX_NAMESPACE

# Create the postgresql backend for UWS data.
kubectl create -f postgresql-deployment.yaml --namespace $DAX_NAMESPACE
kubectl create -f postgresql-service.yaml --namespace $DAX_NAMESPACE

# Create the Presto deployment using Helm.
if [ ! -d "/tmp/presto-chart" ]; then
  # Use this version of the presto chart, which supports creating
  # backends via the values.yaml.
  git clone https://github.com/lotuc/presto-chart.git /tmp/presto-chart
fi

helm install -f presto-helm-values.yaml /tmp/presto-chart/ --name dax-presto-int --namespace $DAX_NAMESPACE

# Create the CADC TAP service.
kubectl create -f tap-service.yaml --namespace $DAX_NAMESPACE
kubectl create -f tap-deployment.yaml --namespace $DAX_NAMESPACE

# Create the ingress rule for incoming requests.
kubectl create -f tap-ingress.yaml --namespace $DAX_NAMESPACE
