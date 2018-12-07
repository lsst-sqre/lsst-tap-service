#!/bin/bash -x

# Delete the Oracle backend.
kubectl delete deployment/oracle-deployment
kubectl delete service/oracle-service

# Delete the mysql backend.
kubectl delete deployment/mysql-deployment
kubectl delete service/mysql-service

# Delete the postgres backend for UWS jobs.
kubectl delete service/postgresql-service
kubectl delete deployment/postgresql-deployment

# Delete the TAP service and ingress rule.
kubectl delete deployment/tap-deployment
kubectl delete service/tap-service
kubectl delete ingress/tap-ingress

# Delete the presto deployment.
helm delete --purge dax
