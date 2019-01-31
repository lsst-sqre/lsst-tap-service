#!/bin/bash -e
kubectl create secret generic google-creds --from-file=./google_creds.json
kubectl create secret generic slack-webhook --from-file=./webhook
