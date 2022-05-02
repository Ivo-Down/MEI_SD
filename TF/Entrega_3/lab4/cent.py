#!/usr/bin/env python3.9

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB()


def broadcast_transaction(msg):
    global node_id, node_ids
    for n in node_ids:
        send(node_id, n, type='txn_broadcast', payload=msg)
    

async def handle(msg):
    # State
    global node_id, node_ids
    global db

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    
    elif msg.body.type == 'txn':
        logging.info('executing txn')
        broadcast_transaction(msg)


    elif msg.body.type == 'txn_broadcast':
        logging.info('received broadcast')
        bcast_msg = msg.body.payload

        # If the node is the one who was contacted by the client
        if msg.src == node_id:
          ctx = await db.begin([k for op,k,v in bcast_msg.body.txn], msg.src+'-'+str(msg.body.msg_id))
          rs,wv,res = await db.execute(ctx,bcast_msg.body.txn)
          if res:
              await db.commit(ctx, wv)
              reply(bcast_msg, type='txn_ok', txn=res)
          else:
              reply(bcast_msg, type='error', code=14, text='transaction aborted')
          db.cleanup(ctx)

        else:
          ctx = await db.begin([k for op,k,v in bcast_msg.body.txn], msg.src+'-'+str(bcast_msg.body.msg_id))
          rs,wv,res = await db.execute(ctx, bcast_msg.body.txn)
          if res:
              await db.commit(ctx, wv)
              send(node_id, msg.src, type='txn_broadcast_ok')
          else:
              send(node_id, msg.src, type='txn_broadcast_false')
          db.cleanup(ctx)


    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
