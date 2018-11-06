#!/bin/bash -x
kubectl delete service/postgresql-service
kubectl delete service/oracle-service
kubectl delete service/tap-service
kubectl delete deployment/postgresql-deployment
kubectl delete deployment/oracle-deployment
kubectl delete deployment/tap-deployment
kubectl delete ingress/tap-ingress
