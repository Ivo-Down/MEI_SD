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
from auxiliars import * 

logging.getLogger().setLevel(logging.DEBUG)
dict = {}
locked = False
next_id = 0
node_locks = {}

quorum_dict = {}

read_response_queue = {}  # contains READ_OK and M_ERROR 20
write_response_queue = {} # contains WRITE_OK and 
cas_response_queue = {}   # contains CAS_OK and CAS_M_ERROR 20 and QR_CAS_COMP_SET and QR_CAS_SET

while True:
    msg = receive()

   

    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        quorum_size, node_ids, node_id = handle_init(msg)
        # Chooses a quorum
        


    # Reads from a quorum and returns the updated value
    elif msg['body']['type'] == M_READ:    
        logging.info('reading key %s', msg['body']['key'])

        # 1 - Choose a read quorum
        key = msg['body']['key']
        request_id = msg['body']['msg_id'] #id associated with this request

        quorum_dict[request_id] = choose_quorum(node_ids, quorum_size)
        read_response_queue.setdefault(request_id, list())
        
        # 2 - Gets the lock for each quorum node
        #get_locks(msg,quorum_dict[key], node_id)

        for node in quorum_dict[msg]:
            sendSimple(msg['body']['src'],msg['src']['dest'],type=QR_READ, request_id=request_id) ## -> ENVIAR O REQUEST_ID PARA CADA QR

            
    elif msg['body']['type'] == M_WRITE:
        if not locked:
            locked = True
            handle_write(msg, quorum_size, node_ids, node_id)
            locked = False
        else:
            errorSimple(msg,type=M_ERROR,code=11,text='Quorum is unavailable.')


    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        if not locked:
            locked = True
            handle_cas(msg,quorum_size,node_ids,node_id)
            locked = False
        else:
            errorSimple(msg,type=M_ERROR,code=11,text='Quorum is unavailable.')


    # Returns the key's timestamp or an error in case the key doesn't exist
    elif msg['body']['type'] == QR_READ:
        logging.info('reading key %s from quorum node %s', msg['body']['key'], msg['src'])
        # CONFIRMAR LOCK, ETC ETC
        key = msg['body']['key']
        if key in dict.keys():
            replySimple(msg,type=QR_READ_OK,value=dict.get(key))
        else:  # When the key does not exist
            errorSimple(msg,type=M_ERROR,code=20,text='Key does not exist')

    elif msg['body']['type'] == QR_READ_OK:
        #some qr nodes might not yet have that value on their dict
        if(msg['body']['value']):
            value_read, timestamp_read = msg['body']['value']
            if timestamp_read > max_timestamp_read:
                updated_value = value_read
                max_timestamp_read = timestamp_read
    


    # Releases the lock
    elif msg['body']['type'] == QR_UNLOCK:
        locked = False

    # Requests the lock, it is already taken sends an error
    elif msg['body']['type'] == QR_LOCK:
        if(not locked):
            locked = True
            replySimple(msg,type=QR_LOCK_OK)
        else:
            replySimple(msg,type=QR_LOCK_FAIL)

    elif msg['body']['type'] == QR_LOCK_OK:
        node_locks[msg['body']['src']] = True 
        check_lock_state(node_locks, node_id, quorum_size, msg, quorum_to_use)
            
                
    elif msg['body']['type'] == QR_LOCK_FAIL:
        node_locks[msg['body']['src']] = False 
        check_lock_state(node_locks, node_id, quorum_size, msg, quorum_to_use)


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


    else:
        logging.warning('unknown message type %s', msg['body']['type'])
