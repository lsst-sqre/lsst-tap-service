#!/bin/bash -ex
# Remove any existing ingress rule and replace it with the
# authorize required ingress rule.
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-int-dax'}

kubectl delete ingress tap-ingress --namespace $DAX_NAMESPACE
kubectl create -f auth-ingress.yaml --namespace $DAX_NAMESPACE
