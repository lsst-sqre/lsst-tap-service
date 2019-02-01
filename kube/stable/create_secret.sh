#!/bin/bash -e
DAX_NAMESPACE=${DAX_NAMESPACE:-'dax-stable'}

kubectl create secret generic google-creds --from-file=./google_creds.json --namespace $DAX_NAMESPACE
kubectl create secret generic slack-webhook --from-file=./webhook --namespace $DAX_NAMESPACE
