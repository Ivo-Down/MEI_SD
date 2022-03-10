#!/usr/bin/env python3
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
from sqlite3 import Timestamp
from time import time
from ms import send, receive

import logging
import math
import random

# -------------------- Auxiliary Functions --------------------

def choose_quorum(list):
  return random.sample(list, quorum_size)


def get_value(dict, key):
  return dict.get(key)


def set_value(dict, key, value, timestamp):
  if(timestamp > dict.get(key)[1]):
    new_value = ( value, timestamp)
    dict[key] = new_value


"""
'qr_reply'
'qr_read'
'qr_read_lock' -> saber o valor do lock
'qr_lock_fail' -> lock já atribuido
'qr_unlock'    -> pedido para dar release ao lock

qr_write
qr_write_ok
"""



logging.getLogger().setLevel(logging.DEBUG)
dict = {}
locked = False


while True:
    msg = receive()
    if not msg:
        break

    if msg['body']['type'] == 'init':
        next_id=1
        node_id = msg['body']['node_id']
        node_ids = msg['body']['node_ids']
        quorum_size = math.ceil((len(node_ids)+1)/2)
        logging.info('node %s initialized', node_id)

        send({
            'dest': msg['src'],
            'src': node_id,
            'body': {
                'type': 'init_ok',
                'msg_id': next_id,
                'in_reply_to': msg['body']['msg_id']
            }
        })
        next_id += 1



    elif msg['body']['type'] == 'read':
        logging.info('reading key %s', msg['body']['key'])
        key = msg['body']['key']

        if key in dict.keys():

            # 1 - Choose a read quorum
            quorum_to_use = choose_quorum(node_ids)
            
            # 2 - Collects pairs (value, timestamp) from each node of the qr
            for node in quorum_to_use:
                send({
                'dest': node,
                'src': node_id,
                'body': {
                    'type': 'qr_read',
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],  #TODO CORRIGIR ISTO
                    'value': key
                }
                })
                next_id += 1
            
            updated_value = 0
            max_timestamp_read = 0

            # 3 - Receives the requested (value, timestamp) from each qr member
            for node in quorum_to_use:
                msg = receive()

                if not msg:
                    break
                
                if msg['body']['type'] == 'qr_reply':  # when an element of qr reads a key and return its value and version
                    value_read, timestamp_read = msg['body']['value']
                    if timestamp_read > max_timestamp_read:
                        updated_value = value_read
                        max_timestamp_read = timestamp_read

            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': 'read_ok',
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],
                    'value': updated_value
                }
            })
            next_id += 1


    elif msg['body']['type'] == 'qr_read':  #reads a value from a qr node
        logging.info('reading key %s from a quorum node', msg['body']['key'])

        key = msg['body']['key']
        send({
            'dest': msg['src'],
            'src': node_id,
            'body': {
                'type': 'qr_reply',
                'msg_id': next_id,
                'in_reply_to': msg['body']['msg_id'],
                'value': dict.get(key)
            }
        })
        next_id += 1


    elif msg['body']['type'] == 'write':
        key = msg['body']['key']
        value = msg['body']['value']
        value_read = 0
        timestamp_read = 0
        client_id = msg['src']
        max_timestamp_read = 0
        updated_value = 0
        logging.info('writing key and value %s %s', key, value)

        # 1 - Choose a write quorum 
        quorum_to_use = choose_quorum(node_ids)

        # 2 - To each node, request timestamp and request lock and saves the highest timestamp
        for node in quorum_to_use:
            send({
                'dest': node,
                'src': node_id,
                'body': {
                    'type': 'qr_read_lock',
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],  #TODO CORRIGIR ISTO
                    'key': key,
                    
                }
            })
            next_id += 1

        failed_locks = []
        
        for node in quorum_to_use:
            msg_aux = receive()
            if not msg_aux:
                break
            
            if msg_aux['body']['type'] == 'qr_reply':  # when an element of qr reads a key and return its value and version
                value_read, timestamp_read = msg_aux['body']['value']
                if timestamp_read > max_timestamp_read:
                    updated_value = value_read
                    max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == 'qr_lock_fail':
                failed_locks.append(msg_aux['src'])

        # 2.1 - If some quorum node doesn't give the lock, it fails and tells the nodes that gave the lock to unlock
        if(failed_locks):
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': 'error',
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 11,
                    'text': 'Write quorum not available'
                }
            })
            next_id += 1
            for lock in failed_locks:
                send({
                    'dest': lock,
                    'src': node_id,
                    'body': {
                        'type': 'qr_unlock',
                        'in_reply_to': msg['body']['msg_id'],
                        'text': 'Release lock request'
                    }
                })
                next_id += 1

        else: 
            # 3 - Write in each qw node
            for node in quorum_to_use:
                timestamp = max_timestamp_read + 1
                to_send = (updated_value, timestamp)
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': 'qr_write',
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id'],
                        'value' : to_send,
                        'key': key
                    }
                })
                next_id += 1


            # 4 - Wait for all the qw node acks
            i = 0
            while i < len(quorum_to_use):
                msg = receive()
                if msg['body']['type'] == 'qr_write_ok':
                    i += 1

    
            # 5 - Return to client
            send({
                'dest': client_id,
                'src': node_id,
                'body': {
                    'type': 'write_ok',
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id']
                }
            })
            next_id += 1


    # Check if lock is available and if it is it returns the key's timestamp
    elif msg['body']['type'] == 'qr_read_lock':
        if(not locked):
            locked = True
            key = msg['body']['key']
            if key in dict.keys():
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': 'qr_reply',
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id'],
                        'value': dict.get(key)
                    }
                })
            else:
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': 'qr_reply',
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id'],
                        'value': (None, 0)
                    }
                })
        else:
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': 'qr_lock_fail',
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id']
                }
            })
        next_id += 1


    # Releases the lock
    elif msg['body']['type'] == 'qr_unlock':
        locked = False


    elif msg['body']['type'] == 'qr_write':

        key = msg['body']['key']
        updated_values = msg['body']['value']

        dict[key] = updated_values

        send({
            'dest': msg['src'],
            'src': node_id,
            'body': {
                'type': 'qr_write_ok',
                'msg_id': next_id,
                'in_reply_to': msg['body']['msg_id']
            }
        })
        next_id += 1

        locked = False


    elif msg['body']['type'] == 'cas_write':

        key = msg['body']['key']
        value_from = msg['body']['from']
        value_to = msg['body']['to']
        real_value = dict.get(key)
        
        if key in dict.keys():
            if value_from == real_value:
                dict[key] = value_to
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': 'cas_write_ok',
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id']
                    }
                })
            else:
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': 'error',
                        'in_reply_to': msg['body']['msg_id'],
                        'code': 22,
                        'text': 'From value does not match'
                    }
                })
        else:
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': 'error',
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 20,
                    'text': 'Key does not exist'
                }
            })
        next_id += 1

        locked = False
        






    # Compares and sets 
    elif msg['body']['type'] == 'cas':
        logging.info('Compare-and-set key %s', msg['body']['key'])
        error = False
        key = msg['body']['key']
        value_read = 0
        timestamp_read = 0
        client_id = msg['src']
        value_from = msg['body']['from']
        value_to = msg['body']['to']

        updated_value = 0
        max_timestamp_read = 0

        # 1 - Choose a write quorum 
        quorum_to_use = choose_quorum(node_ids)

        # 2 - To each node, request timestamp and request lock and saves the highest timestamp
        for node in quorum_to_use:
            send({
                'dest': node,
                'src': node_id,
                'body': {
                    'type': 'qr_read_lock',
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],  #TODO CORRIGIR ISTO
                    'key': key,
                    
                }
            })
            next_id += 1

        failed_locks = []
        
        for node in quorum_to_use:
            msg_aux = receive()
            if not msg_aux:
                break
            
            if msg_aux['body']['type'] == 'qr_reply':  # when an element of qr reads a key and return its value and version
                value_read, timestamp_read = msg_aux['body']['value']
                if timestamp_read > max_timestamp_read:
                    updated_value = value_read
                    max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == 'qr_lock_fail':
                failed_locks.append(msg_aux['src'])

        # 2.1 - If some quorum node doesn't give the lock, it fails and tells the nodes that gave the lock to unlock
        if(failed_locks):
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': 'error',
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 11,
                    'text': 'Write quorum not available'
                }
            })
            next_id += 1
            for lock in failed_locks:
                send({
                    'dest': lock,
                    'src': node_id,
                    'body': {
                        'type': 'qr_unlock',
                        'in_reply_to': msg['body']['msg_id'],
                        'text': 'Release lock request'
                    }
                })
                next_id += 1

        else: 
            # 3 - Compare and set in each qw node
            for node in quorum_to_use:
                timestamp = max_timestamp_read + 1
                to_send = (updated_value, timestamp)
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': 'cas_write',
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id'],
                        'value' : to_send,
                        'key': key,
                        'from': value_from,
                        'to': value_to
                    }
                })
                next_id += 1
        
            # 4 - Wait for all the qw node acks
            i = 0
            while i < len(quorum_to_use):
                msg = receive()
                if msg['body']['type'] == 'qr_write_ok':
                    i += 1

                # 4.1 - If reply is error, return the error to client
                elif msg['body']['type'] == 'error':
                    send(msg)
                    error = True
                    break
    
            
            # 5 - Return to client
            if(not error):
                send({
                    'dest': client_id,
                    'src': node_id,
                    'body': {
                        'type': 'cas_ok',
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id']
                    }
                })
                next_id += 1


        

    else:
        logging.warning('unknown message type %s', msg['body']['type'])

