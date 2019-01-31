#!/bin/bash
# Set up a kubernetes cluster with all the helm charts
# and install tiller

# Set up tiller
kubectl create serviceaccount tiller \
    --namespace=kube-system

kubectl create clusterrolebinding tiller \
    --clusterrole cluster-admin \
    --serviceaccount=kube-system:tiller

helm init --wait --service-account tiller

kubectl patch deployment tiller-deploy \
    --namespace=kube-system \
    --type=json \
    --patch='[{"op": "add", "path": "/spec/template/spec/containers/0/command", "value": ["/tiller", "--listen=localhost:44134"]}]'
