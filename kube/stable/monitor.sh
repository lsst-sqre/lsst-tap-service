#!/bin/bash -ex
# Start the query monkey to monitor the deployment.
# This assumes it is being run at NCSA.
DAX_NAMESPACE=${DAX_NAMESPACE:-'lsst-lsp-stable-dax'}

kubectl create -f querymonkey-deployment.yaml --namespace $DAX_NAMESPACE
