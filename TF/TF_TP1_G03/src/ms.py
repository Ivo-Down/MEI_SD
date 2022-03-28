import json
import logging
import sys
from types import SimpleNamespace as sn

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
        
# TODO: Check if msg_id is correctly set
msg_id = 0

# Usage:
# send(from, to, type='read_ok')
def sendSimple(src, dest, **body):
    global msg_id
    data = json.dumps(sn(dest=dest, src=src, body=sn(msg_id=(msg_id:=msg_id+1), **body)), default=vars)
    logging.debug("sending %s", data)
    print(data, flush=True)

# Usage:
# reply(msg, type='read_ok')
# # reply(msg, type='error', code=22, text='Error Text.')
def replySimple(request, **body):
    sendSimple(request['dest'], request['src'], in_reply_to=request['body']['msg_id'], **body)

# Usage:
# error(msg, type='read_ok')
# # error(msg, type='error', code=22, text='Error Text.')
def errorSimple(request, **body):
    sendSimple(request['dest'], request['src'], in_reply_to=request['body']['msg_id'], **body)