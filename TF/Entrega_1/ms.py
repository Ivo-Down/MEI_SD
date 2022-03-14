import json
import logging
import sys

def send(message):
    data = json.dumps(message)
    logging.debug("sending %s", data)
    print(data)
    sys.stdout.flush()

def receive():
    data = sys.stdin.readline()
    if data:
        logging.debug("received %s", data.strip())
        return json.loads(data)
    else:
        return None