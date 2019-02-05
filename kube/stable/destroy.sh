#!/bin/bash -x
# Destroy the lsst-lsp-int environment.
# This assumes it is being run at NCSA.
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-stable'}

# Delete the Oracle backend.
kubectl delete deployment/oracle-deployment --namespace $DAX_NAMESPACE
kubectl delete service/oracle-service --namespace $DAX_NAMESPACE

# Delete the mysql backend.
kubectl delete deployment/tap-schema-deployment --namespace $DAX_NAMESPACE
kubectl delete service/tap-schema-service --namespace $DAX_NAMESPACE

# Delete the postgres backend for UWS jobs.
kubectl delete service/postgresql-service --namespace $DAX_NAMESPACE
kubectl delete deployment/postgresql-deployment --namespace $DAX_NAMESPACE

# Delete the TAP service and ingress rule.
kubectl delete deployment/tap-deployment --namespace $DAX_NAMESPACE
kubectl delete service/tap-service --namespace $DAX_NAMESPACE
kubectl delete ingress/tap-ingress --namespace $DAX_NAMESPACE

# Delete the presto deployment.
helm delete --purge dax-presto-stable

# Delete the query monkey if it is running
kubectl delete deployment/querymonkey-deployment --namespace $DAX_NAMESPACE
