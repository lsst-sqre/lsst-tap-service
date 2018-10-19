#!/bin/bash -e
kubectl create secret generic google-creds --from-file=./google_creds.json
