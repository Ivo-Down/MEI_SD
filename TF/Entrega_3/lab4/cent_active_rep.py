#!/usr/bin/env python3.9

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB()
requestQueue = []  # Stores messages as we wait for their timestamp to come
processQueue = {}  # Stores messages to process sorted by their timestamp | key: timeStamp, value: clientMsg
nextTimeStamp = 0  # Next message to process


def broadcast_transaction(msg, timeStamp):
    global node_id, node_ids
    for n in node_ids:
        send(node_id, n, type='txn_broadcast', ts=timeStamp, payload=msg)


# Checks if there is a new message to process and processes it if there is
async def checkIfMessageToProcess():
    global nextTimeStamp, processQueue
    if nextTimeStamp in processQueue.keys():
        clientMsg = processQueue[nextTimeStamp]
        await processClientRequest(clientMsg)
        # delete message from queue
        del processQueue[nextTimeStamp]
        nextTimeStamp += 1

    

async def processClientRequest(clientMsg):
    global node_id, db

    # If the node is the one who was contacted by the client, reply to client
    if clientMsg.dest == node_id:
        ctx = await db.begin([k for op,k,v in clientMsg.body.txn], clientMsg.src+'-'+str(clientMsg.body.msg_id))
        rs,wv,res = await db.execute(ctx,clientMsg.body.txn)
        if res:
            await db.commit(ctx, wv)
            reply(clientMsg, type='txn_ok', txn=res)
        else:
            reply(clientMsg, type='error', code=14, text='transaction aborted')
        db.cleanup(ctx)

    else:
        ctx = await db.begin([k for op,k,v in clientMsg.body.txn], clientMsg.dest+'-'+str(clientMsg.body.msg_id))
        rs,wv,res = await db.execute(ctx, clientMsg.body.txn)
        if res:
            await db.commit(ctx, wv)
            send(node_id, clientMsg.dest, type='txn_broadcast_ok')
        else:
            send(node_id, clientMsg.dest, type='txn_broadcast_false')
        db.cleanup(ctx)



async def handle(msg):
    # State
    global node_id, node_ids
    global db
    global requestQueue, processQueue, nextTimeStamp

    await checkIfMessageToProcess()

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    
    elif msg.body.type == 'txn':
        logging.info('executing txn')
        requestQueue.append(msg)
        send(node_id, 'lin-tso', type='ts')


    elif msg.body.type == 'txn_broadcast':
        logging.info('received broadcast')
        # Add new msg to processQueue
        processQueue[msg.body.ts] = msg.body.payload
        # If it contains the next message, processes it
        #checkIfMessageToProcess()

            

    elif msg.body.type == 'ts_ok':
        timeStamp = msg.body.ts
        msgToBroadcast = requestQueue.pop(0)
        broadcast_transaction(msgToBroadcast, timeStamp)



    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
