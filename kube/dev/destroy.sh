#!/bin/bash -x
# If the hostname we're deploying from is at NCSA,
# this string will not be empty, allowing for us to
# do different things for GKE / NCSA.
NCSA_DEPLOY=`hostname -f | grep ncsa`

# Delete the Oracle backend.
kubectl delete deployment/oracle-deployment
kubectl delete service/oracle-service

# Delete the mysql backend.
kubectl delete deployment/tap-schema-deployment
kubectl delete service/tap-schema-service

if [ -z "$NCSA_DEPLOY" ]; then
  # Delete the mock qserv backend.
  kubectl delete deployment/mock-qserv-deployment
  kubectl delete service/qserv-master01
fi

# Delete the postgres backend for UWS jobs.
kubectl delete service/postgresql-service
kubectl delete deployment/postgresql-deployment

# Delete the TAP service and ingress rule.
kubectl delete deployment/tap-deployment
kubectl delete service/tap-service
kubectl delete ingress/tap-ingress

# Delete the presto deployment.
helm delete --purge dax
