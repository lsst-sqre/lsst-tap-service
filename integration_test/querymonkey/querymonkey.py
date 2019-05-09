import argparse
import jinja2
import logging
import os
import time
import random

import pyvo
import requests


def limit_dec(x):
    return max(min(x, 90), -90)

def generate_parameters():
    return {
        'limit_dec': limit_dec,
        'ra': random.uniform(0, 360),
        'dec': random.uniform(-90, 90),
        'r1': random.uniform(0, 1),
        'r2': random.uniform(0, 1),
        'r3': random.uniform(0, 1),
        'r4': random.uniform(0, 1),
        'rsmall': random.uniform(0, .25)
    }

def send_notification(error_count, query, error):
    ERROR_LIMIT = 3
    msg = '[%i] errors in a row. Running [%s] threw [%s]' \
        % (error_count, query, error)
    logging.error(msg)

    if args.slack_webhook and error_count <= ERROR_LIMIT:
        if error_count == ERROR_LIMIT:
            msg = '[%i] errors in a row, squelching' % ERROR_LIMIT
        r = requests.post(args.slack_webhook, json={'text': msg})
        logging.info('Slack webook post returns: %i', r.status_code)


logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Bot to run queries against TAP")
parser.add_argument('--server', required=True)
parser.add_argument('--timer', type=int, default=os.environ.get('timer', 60))
parser.add_argument('--dir', default='test_queries')
parser.add_argument('--dry-run', action='store_true')
parser.add_argument('--slack-webhook')
args = parser.parse_args()

query_templates = os.listdir(args.dir)
service = pyvo.dal.TAPService(args.server)

query_id = 0
success_id = 0

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(args.dir),
    undefined=jinja2.StrictUndefined
)

logging.info("Query templates to choose from: %s", env.list_templates())

while(True):
    query_id += 1

    template_name = random.choice(env.list_templates())
    template = env.get_template(template_name)
    query = template.render(generate_parameters())

    logging.info("[%i] Running: %s", query_id, query)
    start = time.time()

    try:
        if not args.dry_run:
            service.search(query)
        success_id = query_id
    except Exception as e:
        error_count = query_id - success_id
        send_notification(error_count, query, str(e))

    end = time.time()
    logging.info("[%i] Finished, took: %i seconds", query_id, end - start)
    time.sleep(args.timer)
