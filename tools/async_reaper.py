#!/usr/bin/env python
from datetime import datetime, timezone
from google.cloud import storage

storage_client = storage.Client()

bucket_name = "async-results.lsst.codes"

bucket = storage_client.bucket(bucket_name)

files = storage_client.list_blobs(bucket_name)

now = datetime.now(timezone.utc)

for f in files:
    age = now - f.updated
    if age.days > 30:
        print(f"Deleting {f.name} because it's too old.")
        bucket.blob(f.name).delete()
