#!/bin/bash -ex
# Create the oracle backend for TAP data.
kubectl create -f oracle-deployment.yaml
kubectl create -f oracle-service.yaml

# Create the postgresql backend for UWS data.
kubectl create -f postgresql-deployment.yaml
kubectl create -f postgresql-service.yaml

# Create the CADC TAP service.
kubectl create -f tap-service.yaml
kubectl create -f tap-deployment.yaml

# Create the ingress rule for incoming requests.
kubectl create -f tap-ingress.yaml
