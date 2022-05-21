#!/usr/bin/env python3.9

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

logging.getLogger().setLevel(logging.DEBUG)

db = DB()
requestQueue = []
ct = 0  # Timestamp of the last processed/commited message
st = 0
writeValues = [] # pairs (wv, commit timestamp)

def broadcast_transaction(update):
    global node_id, node_ids, ct, st
    for n in node_ids:
        send(node_id, n, type='txn_broadcast', payload=update, ct=ct, st=st)


# Checks if there is an intersection between ct and st
def checkIntersection(txn_rs, txn_ct, txn_st):
    global writeValues, ct
    writeValuesToCompare = []

    for x in writeValues:
        if x[1] > txn_st and x[1] < ct: #DÚVIDA: ct do nodo, ou txn_ct?
            writeValuesToCompare.append(x[0])

    for r in txn_rs:
        if r in writeValuesToCompare:
            return True

    return False



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
    global node_id, db, writeValues, ct

    ctx = await db.begin([k for op,k,v in nextUpdate[3].body.txn], nextUpdate[3].src+'-'+str(nextUpdate[3].body.msg_id))
    rs = nextUpdate[0]
    wv = nextUpdate[1]
    res = nextUpdate[2]

    if res:
        await db.commit(ctx, wv)
        if nextUpdate[3].dest == node_id:
            reply(nextUpdate[3], type='txn_ok', txn=res)
            # Saves the new commit
            writeValues.append((wv, ct))
            ct += 1
            
    else:
        if nextUpdate[3].dest == node_id:
            reply(nextUpdate[3], type='error', code=14, text='transaction aborted on commit')

    db.cleanup(ctx)

    



async def handle(msg):
    # State
    global node_id, node_ids
    global db
    global st

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


    elif msg.body.type == 'txn_broadcast':
        # Validar txn para decidir se faz commit ou descarta 
            # comparar read-set da txn T com os write-values de todos os commits
            # feitos desde que T começou a executar -> faz commit se n houver interseção

        txn_rs = msg.body.payload[0]
        txn_st = msg.body.st
        txn_ct = msg.body.ct

        #Para os writeValues entre txn_ct e txn_st quero ver se ha alguma interseçao com txn_rs
        if not checkIntersection(txn_rs, txn_ct, txn_st):
            await commit(msg.body.payload)

            
    elif msg.body.type == 'ts_ok':
        # When timestamp arrives, sends the first available update with that timestamp to all nodes
        st = msg.body.ts
        updateToBroadcast = requestQueue.pop(0)
        broadcast_transaction(updateToBroadcast)


    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
