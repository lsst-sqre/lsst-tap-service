import argparse
import logging
import os
import time

import pyvo
import requests

logging.basicConfig(level=logging.INFO)

queries = [
    "SELECT ra, decl FROM wise_00.allwise_p3as_mep WHERE CONTAINS(POINT('ICRS', ra, decl), CIRCLE('ICRS', 1.0, -1.0, .1)) = 1"
]

parser = argparse.ArgumentParser(description="Bot to run queries against TAP")
parser.add_argument('--server', default=os.environ.get('server', None))
parser.add_argument('--timer', type=int, default=os.environ.get('timer', 60))
args = parser.parse_args()

service = pyvo.dal.TAPService(args.server)

while(1):
    query = queries[0]
    logging.info("Running query: %s",  query)
    service.search(query)
    logging.info("Finished running query: %s", query)
    time.sleep(args.timer)
