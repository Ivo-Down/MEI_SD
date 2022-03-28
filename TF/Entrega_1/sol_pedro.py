#!/usr/bin/env python3.9
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
import time
from ms import *

import logging
from commands import *
from constants import *
from auxiliars import * 

logging.getLogger().setLevel(logging.DEBUG)
dict = {}
locked = False

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
        request_id = time.time() #id associated with this request

        nodes = choose_quorum(node_ids, quorum_size)
        quorum_dict[request_id] = nodes, msg['src'], msg['body']['msg_id']
        response_queue.setdefault(request_id, list())
        
        for node in nodes:
            sendSimple(node_id, node, type=QR_READ, request_id=request_id, key=key) ## -> ENVIAR O REQUEST_ID PARA CADA QR

    # Returns the key's timestamp or an error in case the key doesn't exist
    elif msg['body']['type'] == QR_READ:
        if not locked:
            locked = True
            request_id = msg['body']['request_id']
            key = msg['body']['key']
            logging.info('reading key %s from quorum node %s', key, msg['src'])
            if key in dict.keys():
                value, timestamp = dict.get(key)
                replySimple(msg,type=QR_READ_OK,value=value,timestamp=timestamp,request_id=request_id)
            else:  # When the key does not exist
                errorSimple(msg,type=QR_READ_FAIL,code=CODE_KEY_MISSING, text='Key does not exist', request_id=request_id)
            locked = False
        else:
            errorSimple(msg,type=QR_READ_FAIL,code=CODE_UNAVAILABLE, text='Node is unavailable', request_id=request_id)
    
    elif msg['body']['type'] == QR_READ_FAIL or msg['body']['type'] == QR_READ_OK:
        request_id = msg['body']['request_id']
        response_queue[request_id].append(msg)
        if len(response_queue[request_id]) == quorum_size:
            _, node_to, in_reply_to  = quorum_dict[request_id]
            if(any(x['body']['type'] == QR_READ_FAIL and int(x['body']['code']) == CODE_UNAVAILABLE for x in response_queue[request_id])):
                # One of the nodes was not available so we cant know if what we have is the most recent value so return error
                sendSimple(node_id, node_to, in_reply_to=in_reply_to, type=M_ERROR,code=CODE_UNAVAILABLE,text='Node is unavailable')
            else:
                # otherwise, try to read the most recent value available (keys can't be deleted)
                r_msg = get_most_recent_msg(filter(lambda x : x['body']['type'] == QR_READ_OK, response_queue[request_id]))
                if r_msg:
                    sendSimple(node_id,node_to, in_reply_to=in_reply_to, type=M_READ_OK, value=r_msg['body']['value'])
                else:
                    sendSimple(node_id,node_to, in_reply_to=in_reply_to, type=M_ERROR, code=CODE_KEY_MISSING, text='Key does not exist')
            
    elif msg['body']['type'] == M_WRITE:
        key = msg['body']['key']
        value = msg['body']['value']
        logging.info('writing key %s', key)

        # 1 - Choose a read quorum
        request_id = time.time() #id associated with this request

        nodes = choose_quorum(node_ids, quorum_size)
        quorum_dict[request_id] = nodes, msg['src'], msg['body']['msg_id']
        response_queue.setdefault(request_id, list())
        
        for node in nodes:
            sendSimple(node_id,node,type=QR_WRITE, request_id=request_id, key=key, value=value) ## -> ENVIAR O REQUEST_ID PARA CADA QR
    
    elif msg['body']['type'] == QR_WRITE:
        if not locked:
            locked = True
            request_id = msg['body']['request_id']
            key = msg['body']['key']
            value = msg['body']['value']
            logging.info('reading key %s from quorum node %s', key, msg['src'])
            if key in dict.keys():
                _, dic_timestamp = dict.get(key)
                if(dic_timestamp < request_id):
                    dict[key] = value, request_id # value, timestamp
                replySimple(msg,type=QR_WRITE_OK, request_id=request_id)
            else:  # When the key does not exist
                dict[key] = value, request_id # value, timestamp
                replySimple(msg,type=QR_WRITE_OK, request_id=request_id)
            locked = False
        else:
            errorSimple(msg,type=QR_WRITE_FAIL,code=CODE_UNAVAILABLE, text='Node is unavailable', request_id=request_id)
    
    elif msg['body']['type'] == QR_WRITE_FAIL or msg['body']['type'] == QR_WRITE_OK:
        request_id = msg['body']['request_id']
        response_queue[request_id].append(msg)
        if len(response_queue[request_id]) == quorum_size:
            _, node_to, in_reply_to = quorum_dict[request_id]
            if(any((x['body']['type'] == QR_WRITE_FAIL and int(x['body']['code']) == CODE_UNAVAILABLE) for x in response_queue[request_id])):
                # One of the nodes was not available so we cant know if what we have is the most recent value so return error
                sendSimple(node_id,node_to,type=M_ERROR, in_reply_to=in_reply_to, code=CODE_UNAVAILABLE,text='Node is unavailable')
            else:
                sendSimple(node_id,node_to, in_reply_to=in_reply_to, type=M_WRITE_OK)

    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        key = msg['body']['key']
        logging.info('reading key %s', msg['body']['key'])

        # 1 - Choose a read quorum
        request_id = time.time() #id associated with this request

        nodes = choose_quorum(node_ids, quorum_size)
        quorum_dict[request_id] = nodes, msg['src'], msg['body']['msg_id']
        response_queue.setdefault(request_id, list())
        
        for node in nodes:
            sendSimple(node_id, node, type=QR_CAS_READ, request_id=request_id, key=key, **{'from' : msg['body']['from']}, to = msg['body']['to']) ## -> ENVIAR O REQUEST_ID PARA CADA QR

    elif msg['body']['type'] == QR_CAS_READ:
        if not locked:
            locked = True
            request_id = msg['body']['request_id']
            key = msg['body']['key']
            logging.info('reading key %s from quorum node %s', key, msg['src'])
            if key in dict.keys():
                value, timestamp = dict.get(key)
                replySimple(msg,type=QR_CAS_READ_OK,value=value,timestamp=timestamp,request_id=request_id, **{'from' : msg['body']['from']}, to = msg['body']['to'])
            else:  # When the key does not exist
                errorSimple(msg,type=QR_CAS_READ_FAIL,code=CODE_KEY_MISSING, text='Key does not exist', request_id=request_id)
            locked = False
        else:
            errorSimple(msg,type=QR_CAS_READ_FAIL,code=CODE_UNAVAILABLE, text='Node is unavailable', request_id=request_id)

    elif msg['body']['type'] == QR_CAS_READ_FAIL or msg['body']['type'] == QR_CAS_READ_OK:
        request_id = msg['body']['request_id']
        response_queue[request_id].append(msg)
        if len(response_queue[request_id]) == quorum_size:
            nodes, node_to, in_reply_to  = quorum_dict[request_id]
            if(any(x['body']['type'] == QR_CAS_READ_FAIL and int(x['body']['code']) == CODE_UNAVAILABLE for x in response_queue[request_id])):
                # One of the nodes was not available so we cant know if what we have is the most recent value so return error
                sendSimple(node_id, node_to,type=M_ERROR, in_reply_to=in_reply_to ,code=CODE_UNAVAILABLE,text='Node is unavailable')
            else:
                # otherwise, try to read the most recent value available (keys can't be deleted)
                r_msg = get_most_recent_msg(filter(lambda x : x['body']['type'] == QR_CAS_READ_OK, response_queue[request_id]))
                if r_msg:
                    if(r_msg['body']['value'] != r_msg['body']['from']):
                        sendSimple(node_id,node_to, in_reply_to=in_reply_to, type=M_ERROR, code=CODE_PRE_COND_FAIL, text='from value differs')
                    else:
                        for node in nodes:
                            response_queue[request_id] = list()
                            sendSimple(node_id,node,type=QR_CAS_WRITE, request_id=request_id, key=key, value=r_msg['body']['to']) ## -> ENVIAR O REQUEST_ID PARA CADA QR
                else:
                    sendSimple(node_id,node_to, in_reply_to=in_reply_to, type=M_ERROR, code=CODE_KEY_MISSING, text='Key does not exist')

    elif msg['body']['type'] == QR_CAS_WRITE:
        if not locked:
            locked = True
            request_id = msg['body']['request_id']
            key = msg['body']['key']
            value = msg['body']['value']
            logging.info('reading key %s from quorum node %s', key, msg['src'])
            if key in dict.keys():
                _, dic_timestamp = dict.get(key)
                if(dic_timestamp < request_id):
                    dict[key] = value, request_id # value, timestamp
                replySimple(msg,type=QR_CAS_WRITE_OK, request_id=request_id)
            else:  # When the key does not exist
                dict[key] = value, request_id # value, timestamp
                replySimple(msg,type=QR_CAS_WRITE_OK, request_id=request_id)
            locked = False
        else:
            errorSimple(msg,type=QR_CAS_WRITE_FAIL,code=CODE_UNAVAILABLE, text='Node is unavailable', request_id=request_id)
    
    elif msg['body']['type'] == QR_CAS_WRITE_FAIL or msg['body']['type'] == QR_CAS_WRITE_OK:
        request_id = msg['body']['request_id']
        response_queue[request_id].append(msg)
        if len(response_queue[request_id]) == quorum_size:
            _, node_to, in_reply_to = quorum_dict[request_id]
            if(any((x['body']['type'] == QR_CAS_WRITE_FAIL and int(x['body']['code']) == CODE_UNAVAILABLE) for x in response_queue[request_id])):
                # One of the nodes was not available so we cant know if what we have is the most recent value so return error
                sendSimple(node_id,node_to,type=M_ERROR, in_reply_to=in_reply_to, code=CODE_UNAVAILABLE,text='Node is unavailable')
            else:
                sendSimple(node_id,node_to, in_reply_to=in_reply_to, type=M_CAS_OK)
    else:
        logging.warning('unknown message type %s', msg['body']['type'])
