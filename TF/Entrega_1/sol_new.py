#!/usr/bin/env python3
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
from sqlite3 import Timestamp
from time import time
from ms import *

import logging
from commands import *
from constants import *

logging.getLogger().setLevel(logging.DEBUG)
dict = {}
locked = False
next_id=0

while True:
    msg = receive()
    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        quorum_size, node_ids, node_id = handle_init(msg)


    # Reads from a quorum and returns the updated value
    elif msg['body']['type'] == M_READ:
        handle_read(msg, quorum_size, node_ids, node_id)


    elif msg['body']['type'] == M_WRITE:
        handle_write(msg, quorum_size, node_ids, node_id)


    # Returns the key's timestamp or an error in case the key doesn't exist
    elif msg['body']['type'] == QR_READ:
        logging.info('reading key %s from quorum node %s', msg['body']['key'], msg['src'])
        if(not locked):
            #locked = True
            key = msg['body']['key']
            if key in dict.keys():
                replySimple(msg,type=QR_READ_OK,value=dict.get(key))
            else:  # When the key does not exist
                errorSimple(msg,type=M_ERROR,code=20,text='Key does not exist')
        else:
            replySimple(msg,type=QR_LOCK_FAIL)


    # Releases the lock
    elif msg['body']['type'] == QR_UNLOCK:
        locked = False

    # Requests the lock, it is already taken sends an error
    elif msg['body']['type'] == QR_LOCK:
        if(not locked):
            locked = True
        else:
            replySimple(msg,type=QR_LOCK_FAIL)


    elif msg['body']['type'] == QR_WRITE:

        key = msg['body']['key']
        updated_values = msg['body']['value']

        dict[key] = updated_values

        replySimple(msg,type=QR_WRITE_OK)

        locked = False


    elif msg['body']['type'] == QR_CAS_COMP_SET:

        key = msg['body']['key']
        value_from = msg['body']['from']
        value_to = msg['body']['to']
        timestamp = msg['body']['timestamp']
        
        if key in dict.keys():
            real_value = dict.get(key)[0]
            if value_from == real_value:
                dict[key] = (value_to, timestamp)
                replySimple(msg,type=QR_CAS_OK)
            else:
                errorSimple(msg,type=M_ERROR,code=22,text='From value does not match')
        else:
            errorSimple(msg, type=M_ERROR, code=20, text='Key does not exist')

        locked = False
    


    # There is no need to compare, only to set
    elif msg['body']['type'] == QR_CAS_SET:

        key = msg['body']['key']
        value_to = msg['body']['to']
        timestamp = msg['body']['timestamp']
        
        if key in dict.keys():
            dict[key] = (value_to, timestamp)
            replySimple(msg, type=QR_CAS_OK) 
        else:
            errorSimple(msg, type=M_ERROR, code=20, text='Key does not exist')
        locked = False

    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        handle_cas(msg,quorum_size,node_ids,node_id)

    else:
        logging.warning('unknown message type %s', msg['body']['type'])

