#!/bin/bash -ex
rm -rf /tmp/datalink_payload.zip /tmp/datalink

if [ -n "$DATALINK_PAYLOAD_URL" ]; then
    curl -L "$DATALINK_PAYLOAD_URL" -o /tmp/datalink_payload.zip
    unzip /tmp/datalink_payload.zip -d /tmp/datalink
fi

exec /usr/bin/cadc-tomcat-start-readonly
