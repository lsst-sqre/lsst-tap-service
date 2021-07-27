#!/usr/bin/env python
from datetime import datetime, timezone
from google.cloud import storage

storage_client = storage.Client()

bucket = storage_client.bucket("async-results.lsst.codes")

files = storage_client.list_blobs("async-results.lsst.codes")

now = datetime.now(timezone.utc)

for f in files:
    print(f"{f.name} created on {f.updated} with size {f.size}")

    age = now - f.updated
    if age.days > 30:
        print(f"Deleting {f.name} because it's too old.")
        bucket.blob(f.name).delete()
