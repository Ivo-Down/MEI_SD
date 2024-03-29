#!/usr/bin/env python3.9
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
import re
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


node_locks = {} # locks para  cada nodo.


#Para cada Pedido (read, write, cas):
quorum_dict = {} # key - id do pedido | value: quorum (lista de nodos)

response_queue = {}  # contains queue de "mini eventos" | key (request id) - value (a resposta)



while True:
    msg = receive()

    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        quorum_size, node_ids, node_id = handle_init(msg)
        # Chooses a quorum
        
    
    # Reads from a quorum and returns the updated value
    elif msg['body']['type'] == M_READ:    
        key = msg['body']['key']
        logging.info('reading key %s', msg['body']['key'])

        # 1 - Choose a read quorum
        request_id = (msg['body']['msg_id'], msg['src']) #id associated with this request

        quorum_dict[request_id] = choose_quorum(node_ids, quorum_size)
        response_queue.setdefault(request_id, list())
        

        for node in quorum_dict[request_id]:
            sendSimple(node_id,node,type=QR_READ, request_id=request_id, key=key) ## -> ENVIAR O REQUEST_ID PARA CADA QR

            
    elif msg['body']['type'] == M_WRITE:
        
        logging.info('reading key %s', msg['body']['key'])
        key = msg['body']['key']

        # 1 - Choose a write quorum
        # request_id = msg['body']['msg_id'] #id associated with this request
        request_id = (msg['body']['msg_id'], msg['src'])

        quorum_dict[request_id] = choose_quorum(node_ids, quorum_size)
        response_queue.setdefault(request_id, list())
        
        for node in quorum_dict[request_id]:
            sendSimple(node_id,node,type=QR_READ_WRITE, request_id=request_id, key=key) ## -> ENVIAR O REQUEST_ID PARA CADA QR


        errorSimple(msg,type=M_ERROR,code=11,text='Quorum is unavailable.')


    # Compares and sets 
    elif msg['body']['type'] == M_CAS:

        # if not locked:
        #     locked = True
        #     handle_cas(msg,quorum_size,node_ids,node_id)
        #     locked = False
        # else:
        #     errorSimple(msg,type=M_ERROR,code=11,text='Quorum is unavailable.')

        errorSimple(msg,type=M_ERROR,code=11,text='Quorum is unavailable.')

        


    # Returns the key's timestamp or an error in case the key doesn't exist
    elif msg['body']['type'] == QR_READ:
        if not locked:
            request_id = tuple(msg['body']['request_id'])
            locked = True
            logging.info('reading key %s from quorum node %s', msg['body']['key'], msg['src'])
            key = msg['body']['key']
            if key in dict.keys():
                replySimple(msg,type=QR_READ_OK,value=dict.get(key),request_id=request_id)
            else:  # When the key does not exist
                errorSimple(msg,type=QR_ERROR,code=20,text='Key does not exist',request_id=request_id)
            locked = False
        else:
            errorSimple(msg,type=QR_ERROR,code=11,text='Node is unavailable.',request_id=request_id)

    
    elif msg['body']['type'] == QR_READ_OK:
        #some qr nodes might not yet have that value on their dict
        request_id = tuple(msg['body']['request_id'])
        response_queue[request_id].append(msg)

        if len(response_queue[request_id]) == quorum_size:
            if not any(x['body']['code'] == 11 for x in response_queue[request_id]):
                max_timestamp_read = -1
                updated_value = 0
                for x in response_queue[request_id]:
                    if not x['body']['type'] == QR_ERROR:
                        value_read, timestamp_read, id = x['body']['value']
                        if timestamp_read > max_timestamp_read:
                            updated_value = value_read
                            max_timestamp_read = timestamp_read
                        elif timestamp_read == max_timestamp_read:
                            if id > node_id:
                                updated_value = value_read
                if max_timestamp_read == -1:
                    sendSimple(node_id,request_id[1],type=M_ERROR,code=20,text='Quorum unavailable with error 20.')
                else: 
                    sendSimple(node_id,request_id[1],type=M_READ_OK,value=updated_value)
            else:
                sendSimple(node_id,request_id[1],type=M_ERROR,code=11,text='Quorum unavailable with error 11.')


    elif msg['body']['type'] == QR_READ_WRITE:
        ## fazer o processo similar, mas guardar o maior timestamp
        if not locked:
            request_id = tuple(msg['body']['request_id'])
            locked = True
            
            logging.info('reading key %s from quorum node %s', msg['body']['key'], msg['src'])
            key = msg['body']['key']
            if key in dict.keys():
                replySimple(msg,type=QR_READ_WRITE_OK,value=dict.get(key),request_id=request_id)
            else:  # When the key does not exist
                errorSimple(msg,type=QR_ERROR_WRITE,code=20,text='Key does not exist',request_id=request_id)
            locked = False
        else:
            errorSimple(msg,type=QR_ERROR_WRITE,code=11,text='Node is unavailable.',request_id=request_id)

    elif msg['body']['type'] == QR_READ_WRITE_OK:
        #some qr nodes might not yet have that value on their dict
        request_id = tuple(msg['body']['request_id'])
        key = msg['body']['key']

        max_timestamp_read = 0
        updated_value = 0

        response_queue[request_id].append(msg)
        if len(response_queue[request_id]) == quorum_size:            
            if not any(x['body']['code'] == 11 for x in response_queue[request_id]):
                for x in response_queue[request_id]:
                    value_read, timestamp_read, id = x['body']['value']
                    if timestamp_read > max_timestamp_read:
                        updated_value = value_read
                        max_timestamp_read = timestamp_read
                    elif timestamp_read == max_timestamp_read:
                        if id > node_id:
                            updated_value = value_read
            else:
                errorSimple(msg,type=QR_ERROR_WRITE,code=11,text='Node is unavailable.',request_id=request_id)

        timestamp = max_timestamp_read + 1
        to_send = (updated_value, timestamp, msg['body']['src'])
        for node in quorum_dict[request_id]:
            sendSimple(node_id,node,type=QR_WRITE,key=key,value=to_send)
            ##TODO:  ------------------ FICAMOS AQUI -------------------------------

    
    elif msg['body']['type'] == QR_ERROR:
        request_id = tuple(msg['body']['request_id'])
        response_queue[request_id].append(msg)
        if len(response_queue[request_id]) == quorum_size:
            if any(x['body']['code'] == 11 for x in response_queue[request_id]):
                sendSimple(node_id,request_id[1],type=M_ERROR,code=11,text='Quorum unavailable with error 11.')
            else:
                max_timestamp_read = -1
                updated_value = 0
                for x in response_queue[request_id]:
                    if not x['body']['type'] == QR_ERROR:
                        value_read, timestamp_read, id = x['body']['value']
                        if timestamp_read > max_timestamp_read:
                            updated_value = value_read
                            max_timestamp_read = timestamp_read
                        elif timestamp_read == max_timestamp_read:
                            if id > node_id:
                                updated_value = value_read

                if max_timestamp_read == -1:
                    sendSimple(node_id,request_id[1],type=M_ERROR,code=20,text='Quorum unavailable with error 20.')
                else: 
                    sendSimple(node_id,request_id[1],type=M_READ_OK,value=updated_value)




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
