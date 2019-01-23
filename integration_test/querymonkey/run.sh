#!/bin/bash -ex
cd "$(dirname ${BASH_SOURCE[0]})"
python querymonkey.py --server $TAP_URL --slack-webhook $SLACK_WEBHOOK
