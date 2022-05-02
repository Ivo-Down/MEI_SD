# Minimal support for Maelstrom node programs

import logging
from sys import stdin
from json import loads, dumps
from types import SimpleNamespace as sn
from asyncio import get_event_loop, create_task

msg_id = 0

def send(src, dest, **body):
    global msg_id
    data = dumps(sn(dest=dest, src=src, body=sn(msg_id=(msg_id:=msg_id+1), **body)), default=vars)
    logging.debug("sending %s", data)
    print(data, flush=True)
    return msg_id

def reply(request, **body):
    return send(request.dest, request.src, in_reply_to=request.body.msg_id, **body)

async def receiveAll(handle):
    while data := await get_event_loop().run_in_executor(None, stdin.readline):
        logging.debug("received %s", data.strip())
        create_task(handle(loads(data, object_hook=lambda x: sn(**x))))
