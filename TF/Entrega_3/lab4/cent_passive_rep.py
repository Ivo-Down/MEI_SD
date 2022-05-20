#!/usr/bin/env python3.9

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

logging.getLogger().setLevel(logging.DEBUG)

db = DB()
requestQueue = []  # Stores (execution info + client message) as we wait for their timestamp to come
updateQueue = {}  # Stores updates to process sorted by their timestamp | key: timeStamp, value: clientMsg
nextTimeStamp = 0  # Next update to process



def broadcast_transaction(update, timeStamp):
    global node_id, node_ids
    for n in node_ids:
        send(node_id, n, type='txn_broadcast', ts=timeStamp, payload=update)


# Checks if there is a new update to process and processes it if there is
async def checkIfUpdateToProcess():
    global nextTimeStamp, updateQueue
    if nextTimeStamp in updateQueue.keys():
        nextUpdate = updateQueue[nextTimeStamp]
        await commit(nextUpdate)
        del updateQueue[nextTimeStamp]
        nextTimeStamp += 1



async def execute(clientMsg):
    global node_id, db, requestQueue

    ctx = await db.begin([k for op,k,v in clientMsg.body.txn], clientMsg.src+'-'+str(clientMsg.body.msg_id))
    rs, wv, res = await db.execute(ctx,clientMsg.body.txn)
    
    if res:
        requestQueue.append((rs, wv, res, clientMsg))
        send(node_id, 'lin-tso', type='ts')  # Gets the order for the next message
    else:
        reply(clientMsg, type='error', code=14, text='transaction aborted on execute')

    db.cleanup(ctx)

    
 

async def commit(nextUpdate):
    global node_id, db

    ctx = await db.begin([k for op,k,v in nextUpdate[3].body.txn], nextUpdate[3].src+'-'+str(nextUpdate[3].body.msg_id))
    rs = nextUpdate[0]
    wv = nextUpdate[1]
    res = nextUpdate[2]

    if res:
        await db.commit(ctx, wv)
        if nextUpdate[3].dest == node_id:
         reply(nextUpdate[3], type='txn_ok', txn=res)
    else:
        if nextUpdate[3].dest == node_id:
            reply(nextUpdate[3], type='error', code=14, text='transaction aborted on commit')

    db.cleanup(ctx)

    



async def handle(msg):
    # State
    global node_id, node_ids
    global db
    global requestQueue, updateQueue, nextTimeStamp

    await checkIfUpdateToProcess()

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    
    elif msg.body.type == 'txn':
        logging.info('executing txn')
        # Execute the operation and gets its timestamp
        await execute(msg)
        await checkIfUpdateToProcess()


    elif msg.body.type == 'txn_broadcast':
        logging.info('received broadcast')
        # Add new update to updateQueue
        updateQueue[msg.body.ts] = msg.body.payload
        # Processes the next update if it already has arrived
        await checkIfUpdateToProcess()

            
    elif msg.body.type == 'ts_ok':
        # When timestamp arrives, sends the first available update with that timestamp to all nodes
        timeStamp = msg.body.ts
        updateToBroadcast = requestQueue.pop(0)
        broadcast_transaction(updateToBroadcast, timeStamp)


    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
