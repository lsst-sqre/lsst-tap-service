#!/bin/bash -ex
# If the hostname we're deploying from is at NCSA,
# this string will not be empty, allowing for us to
# do different things for GKE / NCSA.
# Since grep returns exit code 1 if not found,
# which makes the script exit, always return true.
NCSA_DEPLOY=`hostname -f | grep ncsa || true`

# Create the oracle backend for TAP data.
kubectl create -f oracle-deployment.yaml
kubectl create -f oracle-service.yaml

# Create the backend for TAP_SCHEMA data.
kubectl create -f tap-schema-deployment.yaml
kubectl create -f tap-schema-service.yaml

if [ -z "$NCSA_DEPLOY" ]; then
  # Create the mock qserv backend if we're not at NCSA
  kubectl create -f mock-qserv-deployment.yaml
  kubectl create -f mock-qserv-service.yaml
fi

# Create the postgresql backend for UWS data.
kubectl create -f postgresql-deployment.yaml
kubectl create -f postgresql-service.yaml

# Create the Presto deployment using Helm.
if [ ! -d "/tmp/presto-chart" ]; then
  # Use this version of the presto chart, which supports creating
  # backends via the values.yaml.
  git clone https://github.com/lotuc/presto-chart.git /tmp/presto-chart
fi

helm install -f presto-helm-values.yaml /tmp/presto-chart/ --name dax

# Create the CADC TAP service.
kubectl create -f tap-service.yaml
kubectl create -f tap-deployment.yaml

if [ -z "$NCSA_DEPLOY" ]; then
  # Create the ingress rule for incoming requests.
  kubectl create -f tap-ingress.yaml
else
  # Create the ingress rule containing the NCSA hostname.
  kubectl create -f tap-ingress-ncsa.yaml
fi
