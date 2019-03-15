#!/bin/bash
# checkAvailability.sh
# Check to make sure that the TAP service came up
# and a query can be executed.
set -e

# Max query attempts
MAX_TRIES=20

# Return true-like values if and only if curl output
# contain the expected "ready" line
function available() {
  curl -vL http://localhost:8080/tap/availability | grep "The TAP ObsCore service is accepting queries"
}
function query() {
  curl -Ls -o /dev/null -w "%{http_code}" -d 'QUERY=SELECT+TOP+1+*+FROM+TAP_SCHEMA.schemas&LANG=ADQL' http://localhost:8080/tap/sync | grep 200
}

function waitUntilServiceIsReady() {
  attempt=1
  while [ $attempt -le $MAX_TRIES ]; do
    if "$@"; then
      echo "$2 check is good!"
      break
    fi
    echo "Waiting to try $2 (attempt: $((attempt++)))"
    sleep 5
  done

  if [ $attempt -gt $MAX_TRIES ]; then
    echo "Error: $2 not responding"
    exit 1
  fi
}

waitUntilServiceIsReady available "Available"
waitUntilServiceIsReady query "SYNC query"
