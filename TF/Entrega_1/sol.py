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

# -------------------- Constants --------------------

# Default

M_INIT = 'init'
M_INIT_OK = 'init_ok'

M_READ = 'read'
M_READ_OK = 'read_ok'

M_WRITE = 'write'
M_WRITE_OK = 'write_ok'

M_CAS = 'cas'
M_CAS_OK = 'cas_ok'

M_ERROR = 'error'

# Custom

QR_REPLY = 'qr_reply'
QR_READ = 'qr_read'
QR_READ_LOCK = 'qr_read_lock'   # -> saber o valor do lock
QR_LOCK_FAIL = 'qr_lock_fail'   # -> lock já atribuido
QR_UNLOCK = 'qr_unlock'         # -> pedido para dar release ao lock

QR_WRITE = 'qr_write'
QR_WRITE_OK = 'qr_write_ok'

QR_CAS_COMP_SET = 'cas_aux'     # compares and sets the value
QR_CAS_SET = 'cas_aux2'         # sets the value when the version more recent


logging.getLogger().setLevel(logging.DEBUG)
dict = {}
locked = False


while True:
    msg = receive()
    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        next_id=1
        node_id = msg['body']['node_id']
        node_ids = msg['body']['node_ids']
        quorum_size = math.ceil((len(node_ids)+1)/2)
        logging.info('node %s initialized', node_id)

        send({
            'dest': msg['src'],
            'src': node_id,
            'body': {
                'type': M_INIT_OK,
                'msg_id': next_id,
                'in_reply_to': msg['body']['msg_id']
            }
        })
        next_id += 1


    # Reads from a quorum and returns the updated value
    elif msg['body']['type'] == M_READ:

        # VARIÁVEIS
        updated_value = 0
        max_timestamp_read = 0
        failed_locks = []

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
                    'type': QR_READ_LOCK,
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],  #TODO CORRIGIR ISTO
                    'key': key
                }
                })
                next_id += 1
            
            
            # 3 - Receives the requested (value, timestamp) from each qr member
            for node in quorum_to_use:
                msg_aux = receive()

                if not msg_aux:
                    break
                
                if msg_aux['body']['type'] == QR_REPLY:  # when an element of qr reads a key and return its value and version
                    if(msg_aux['body']['value']): # some qr nodes might not yet have that value on their dict
                        value_read, timestamp_read = msg_aux['body']['value']
                        if timestamp_read > max_timestamp_read:
                            updated_value = value_read
                            max_timestamp_read = timestamp_read

                elif msg_aux['body']['type'] == QR_LOCK_FAIL:
                    failed_locks.append(msg_aux['src'])

            
            

            # 4 - If some quorum node doesn't give the lock, it fails and tells the other nodes (which gave the lock) to unlock
            if(failed_locks):
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': M_ERROR,
                        'in_reply_to': msg['body']['msg_id'],
                        'code': 11,  #TODO VER QUE ERRO VAI AQUI
                        'text': 'Read quorum not available'
                    }
                })
                next_id += 1
                for lock in list(set(quorum_to_use) - set(failed_locks)):
                    send({
                        'dest': lock,
                        'src': node_id,
                        'body': {
                            'type': QR_UNLOCK,
                            'in_reply_to': msg['body']['msg_id'],
                            'text': 'Release lock request'
                        }
                    })
                    next_id += 1

            else: 
                # Replies to the client, everything is ok
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': M_READ_OK,
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id'],
                        'value': updated_value
                    }
                })
                next_id += 1

        


    elif msg['body']['type'] == QR_READ:  #reads a value from a qr node
        logging.info('reading key %s from quorum node %s', msg['body']['key'], msg['src'])
        key = msg['body']['key']
        send({
            'dest': msg['src'],
            'src': node_id,
            'body': {
                'type': QR_REPLY,
                'msg_id': next_id,
                'in_reply_to': msg['body']['msg_id'],
                'value': dict.get(key)
            }
        })
        next_id += 1


    elif msg['body']['type'] == M_WRITE:
        failed_locks = []
        value_read = 0
        timestamp_read = 0
        max_timestamp_read = 0
        
        key = msg['body']['key']
        value = msg['body']['value']
        
        client_id = msg['src']
        
        logging.info('writing key and value %s %s', key, value)

        # 1 - Choose a write quorum 
        quorum_to_use = choose_quorum(node_ids)

        # 2 - To each node, request timestamp and request lock and saves the highest timestamp
        for node in quorum_to_use:
            send({
                'dest': node,
                'src': node_id,
                'body': {
                    'type': QR_READ_LOCK,
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],  #TODO CORRIGIR ISTO
                    'key': key,
                    
                }
            })
            next_id += 1

       
        
        for node in quorum_to_use:
            msg_aux = receive()
            if not msg_aux:
                break
            
            if msg_aux['body']['type'] == QR_REPLY:  # when an element of qr reads a key and return its value and version
                value_read, timestamp_read = msg_aux['body']['value']
                if timestamp_read > max_timestamp_read:
                    max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == QR_LOCK_FAIL:
                failed_locks.append(msg_aux['src'])

        # 2.1 - If some quorum node doesn't give the lock, it fails and tells the nodes that gave the lock to unlock
        if(failed_locks):
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': M_ERROR,
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 11,
                    'text': 'Write quorum not available'
                }
            })
            next_id += 1
            for lock in list(set(quorum_to_use) - set(failed_locks)):
                send({
                    'dest': lock,
                    'src': node_id,
                    'body': {
                        'type': QR_UNLOCK,
                        'in_reply_to': msg['body']['msg_id'],
                        'text': 'Release lock request'
                    }
                })
                next_id += 1

        else: 
            # 3 - Write in each qw node
            for node in quorum_to_use:
                timestamp = max_timestamp_read + 1
                to_send = (value, timestamp)
                send({
                    'dest': node,
                    'src': node_id,
                    'body': {
                        'type': QR_WRITE,
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
                if msg['body']['type'] == QR_WRITE_OK:
                    i += 1

    
            # 5 - Return to client
            send({
                'dest': client_id,
                'src': node_id,
                'body': {
                    'type': M_WRITE_OK,
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id']
                }
            })
            next_id += 1


    # Check if lock is available and if it is it returns the key's timestamp
    elif msg['body']['type'] == QR_READ_LOCK:
        if(not locked):
            locked = True
            key = msg['body']['key']
            if key in dict.keys():
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': QR_REPLY,
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
                        'type': QR_REPLY,
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
                    'type': QR_LOCK_FAIL,
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id']
                }
            })
        next_id += 1


    # Releases the lock
    elif msg['body']['type'] == QR_UNLOCK:
        locked = False


    elif msg['body']['type'] == QR_WRITE:

        key = msg['body']['key']
        updated_values = msg['body']['value']

        dict[key] = updated_values

        send({
            'dest': msg['src'],
            'src': node_id,
            'body': {
                'type': QR_WRITE_OK,
                'msg_id': next_id,
                'in_reply_to': msg['body']['msg_id']
            }
        })
        next_id += 1

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
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': M_CAS_OK,
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id']
                    }
                })
            else:
                send({
                    'dest': msg['src'],
                    'src': node_id,
                    'body': {
                        'type': M_ERROR,
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
                    'type': M_ERROR,
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 20,
                    'text': 'Key does not exist'
                }
            })
        next_id += 1

        locked = False
    


    # There is no need to compare, only to set
    elif msg['body']['type'] == QR_CAS_SET:

        key = msg['body']['key']
        value_to = msg['body']['to']
        timestamp = msg['body']['timestamp']
        
        if key in dict.keys():
            dict[key] = (value_to, timestamp)
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': M_CAS_OK,
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id']
                }
            })     
        else:
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': M_ERROR,
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 20,
                    'text': 'Key does not exist'
                }
            })
        next_id += 1
        locked = False






    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        logging.info('Compare-and-set key %s', msg['body']['key'])
        error = False
        key = msg['body']['key']
        value_read = 0
        timestamp_read = 0
        max_timestamp_read = 0
        client_id = msg['src']
        value_from = msg['body']['from']
        value_to = msg['body']['to']


        # 1 - Choose a quorum 
        quorum_to_use = choose_quorum(node_ids)

        # 2 - To each node, request timestamp and request lock and saves the highest timestamp
        for node in quorum_to_use:
            send({
                'dest': node,
                'src': node_id,
                'body': {
                    'type': QR_READ_LOCK,
                    'msg_id': next_id,
                    'in_reply_to': msg['body']['msg_id'],  #TODO CORRIGIR ISTO
                    'key': key,
                    
                }
            })
            next_id += 1

        failed_locks = []
        node_info = {}  #key: node_id,  value: (value, timestamp)
        
        for node in quorum_to_use:
            msg_aux = receive()
            if not msg_aux:
                break
            
            if msg_aux['body']['type'] == QR_REPLY:  # when an element of qr reads a key and return its value and version
                value_read, timestamp_read = msg_aux['body']['value']
                node_info[node] = msg_aux['body']['value']
                if timestamp_read > max_timestamp_read:
                    max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == QR_LOCK_FAIL:
                failed_locks.append(msg_aux['src'])

        # 2.1 - If some quorum node doesn't give the lock, it fails and tells the nodes that gave the lock to unlock
        if(failed_locks):
            send({
                'dest': msg['src'],
                'src': node_id,
                'body': {
                    'type': M_ERROR,
                    'in_reply_to': msg['body']['msg_id'],
                    'code': 11,
                    'text': 'Quorum not available for CAS'
                }
            })
            next_id += 1
            for lock in list(set(quorum_to_use) - set(failed_locks)):
                send({
                    'dest': lock,
                    'src': node_id,
                    'body': {
                        'type': QR_UNLOCK,
                        'in_reply_to': msg['body']['msg_id'],
                        'text': 'Release lock request'
                    }
                })
                next_id += 1

        else: 
            # 3 - Compare and set the nodes which have the latest version
            for node in quorum_to_use:
                if(node_info.get(node)[1] == max_timestamp_read):
                    timestamp = max_timestamp_read + 1
                    send({
                        'dest': node,
                        'src': node_id,
                        'body': {
                            'type': QR_CAS_COMP_SET,
                            'msg_id': next_id,
                            'in_reply_to': msg['body']['msg_id'],
                            'key': key,
                            'from': value_from,
                            'to': value_to,
                            'timestamp': timestamp
                        }
                    })
                    next_id += 1
                else:
                    send({
                        'dest': node,
                        'src': node_id,
                        'body': {
                            'type': QR_CAS_SET,
                            'msg_id': next_id,
                            'in_reply_to': msg['body']['msg_id'],
                            'key': key,
                            'to': value_to,
                            'timestamp': timestamp
                        }
                    })
                    next_id += 1
                
        
            # 4 - Wait for all the qw node acks
            i = 0
            while i < len(quorum_to_use):
                msg = receive()
                if msg['body']['type'] == QR_WRITE_OK:
                    i += 1

                # 4.1 - If reply is error, return the error to client
                elif msg['body']['type'] == M_ERROR:
                    send(msg)
                    error = True
                    break
    
            
            # 5 - Return to client
            if(not error):
                send({
                    'dest': client_id,
                    'src': node_id,
                    'body': {
                        'type': M_CAS_OK,
                        'msg_id': next_id,
                        'in_reply_to': msg['body']['msg_id']
                    }
                })
                next_id += 1


    else:
        logging.warning('unknown message type %s', msg['body']['type'])

