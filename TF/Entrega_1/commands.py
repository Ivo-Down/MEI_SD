from auxiliars import choose_quorum, get_locks
from ms import *
import math
import logging
from constants import *

logging.getLogger().setLevel(logging.DEBUG)

# Initializes the Node
# Returns the quorum size, node_ids, node_id
def handle_init(msg):
    node_id = msg['body']['node_id']
    node_ids = msg['body']['node_ids']
    logging.info('node %s initialized', node_id)

    #msg['dest'] = msg['src']
    #msg['src'] = node_id
    replySimple(msg, type=M_INIT_OK)
    return math.ceil((len(node_ids)+1)/2), node_ids, node_id

# Handles the read command
def handle_read(msg, quorum_size, node_ids, node_id):
    # VARIÁVEIS
    updated_value = 0
    max_timestamp_read = 0
    failed_locks = []
    nodes_to_unlock = []

    logging.info('reading key %s', msg['body']['key'])
    key = msg['body']['key']

    # 1 - Choose a read quorum
    quorum_to_use = choose_quorum(node_ids, node_id, quorum_size)

    # 2 - Gets the lock for each quorum node
    results = get_locks(msg,quorum_to_use, node_id)

    if(results):
        # 2 - Collects pairs (value, timestamp) from each node of the qr
        for node in quorum_to_use:
            replySimple(msg, type=QR_READ, key=key)

        # 3 - Receives the requested (value, timestamp) from each qr member
        for node in quorum_to_use:
            msg_aux = receive()

            if not msg_aux:
                break

            # when an element of qr reads a key and return its value and version
            if msg_aux['body']['type'] == QR_READ_OK:
                # some qr nodes might not yet have that value on their dict
                if(msg_aux['body']['value']):
                    value_read, timestamp_read = msg_aux['body']['value']
                    if timestamp_read > max_timestamp_read:
                        updated_value = value_read
                        max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == QR_LOCK_FAIL:
                failed_locks.append(msg_aux['src'])

            elif msg_aux['body']['type'] == M_ERROR:  # key does not exist
                nodes_to_unlock.append(msg_aux['src'])

        for n in nodes_to_unlock:
            sendSimple(node_id,n,type=QR_UNLOCK)

        # 4 - If some quorum node doesn't give the lock, it fails and tells the other nodes (which gave the lock) to unlock
        if(failed_locks):
            errorSimple(msg, type=M_ERROR, code=11,
                        text='Read quorum not available')
            for lock in list(set(quorum_to_use) - set(failed_locks)):
                sendSimple(node_id, lock, type=QR_UNLOCK,
                           text='Release lock request')

        else:
            # Replies to the client, everything is ok
            replySimple(type=M_READ_OK, value=updated_value)

# Handles the write command
def handle_write(msg, quorum_size, node_ids, node_id):
        failed_locks = []
        value_read = 0
        timestamp_read = 0
        max_timestamp_read = 0
        
        key = msg['body']['key']
        value = msg['body']['value']
        
        logging.info('writing key and value %s %s', key, value)

        # 1 - Choose a write quorum 
        quorum_to_use = choose_quorum(node_ids, node_id, quorum_size)

        # 2 - To each node, request timestamp and request lock and saves the highest timestamp
        for node in quorum_to_use:
            sendSimple(node_id,node,type=QR_READ,key=key)
        
        for node in quorum_to_use:
            msg_aux = receive()
            if not msg_aux:
                break
            
            if msg_aux['body']['type'] == QR_READ_OK:  # when an element of qr reads a key and return its value and version
                value_read, timestamp_read = msg_aux['body']['value']
                if timestamp_read > max_timestamp_read:
                    max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == QR_LOCK_FAIL:
                failed_locks.append(msg_aux['src'])

        # 2.1 - If some quorum node doesn't give the lock, it fails and tells the nodes that gave the lock to unlock
        if(failed_locks):
            replySimple(type=M_ERROR,code=11,text='Write quorum not available')
            for lock in list(set(quorum_to_use) - set(failed_locks)):
                sendSimple(node_id, lock, type=QR_UNLOCK,text = 'Release lock request')

        else: 
            # 3 - Write in each qw node
            for node in quorum_to_use:
                timestamp = max_timestamp_read + 1
                to_send = (value, timestamp)
                sendSimple(node_id,node,type=QR_WRITE,key=key,value=to_send)

            # 4 - Wait for all the qw node acks
            i = 0
            while i < len(quorum_to_use):
                msg = receive()
                if msg['body']['type'] == QR_WRITE_OK:
                    i += 1
    
            # 5 - Return to client
            replySimple(msg,type=M_WRITE_OK)


# Handles the write command
def handle_cas(msg, quorum_size, node_ids, node_id):
        logging.info('Compare-and-set key %s', msg['body']['key'])
        error = False
        key = msg['body']['key']
        value_read = 0
        timestamp_read = 0
        max_timestamp_read = 0
        value_from = msg['body']['from']
        value_to = msg['body']['to'] 
        failed_locks = []
        nodes_to_unlock = []
        node_info = {}  #key: node_id,  value: (value, timestamp)


        # 1 - Choose a quorum 
        quorum_to_use = choose_quorum(node_ids, node_id, quorum_size)

        # 2 - To each node, request timestamp and request lock and saves the highest timestamp
        for node in quorum_to_use:
            sendSimple(node_id,node,type=QR_READ,key=key)
        
        for node in quorum_to_use:
            msg_aux = receive()
            if not msg_aux:
                break
            
            if msg_aux['body']['type'] == QR_READ_OK:  # when an element of qr reads a key and return its value and version
                value_read, timestamp_read = msg_aux['body']['value']
                node_info[node] = (value_read, timestamp_read)
                if timestamp_read > max_timestamp_read:
                    max_timestamp_read = timestamp_read

            elif msg_aux['body']['type'] == QR_LOCK_FAIL:
                failed_locks.append(msg_aux['src'])

            elif msg_aux['body']['type'] == M_ERROR: # key does not exist
                nodes_to_unlock.append(msg_aux['src'])
                error = True

        if(nodes_to_unlock):
            for n in nodes_to_unlock:
                sendSimple(node_id,n,type=QR_UNLOCK)
            errorSimple(msg,type=M_ERROR,code=20,text='Key does not exist!')
        
        else: #If there were no errors of type: 'key does not exist'

            # 2.1 - If some quorum node doesn't give the lock, it fails and tells the nodes that gave the lock to unlock
            if(failed_locks):
                errorSimple(msg,type=M_ERROR,code=11,text='Quorum not available for CAS (lock problem)')
                for lock in list(set(quorum_to_use) - set(failed_locks)):
                    sendSimple(node_id,lock,type=QR_UNLOCK,text='Release Lock Request.')

            else:  # 3 - Compare and set the nodes which have the latest version
                for node in quorum_to_use:
                    if(node in node_info):
                        timestamp = max_timestamp_read + 1
                        if(node_info.get(node)[1] == max_timestamp_read):
                            sendSimple(node_id,node,type=QR_CAS_COMP_SET,key=key,**{'from': value_from},to=value_to,timestamp=timestamp) 
                        else:
                            sendSimple(node_id,node,type=QR_CAS_SET,key=key,to=value_to,timestamp=timestamp)
            
                # 4 - Wait for all the qw node acks
                i = 0
                while i < len(quorum_to_use):
                    msg = receive()
                    if msg['body']['type'] == QR_CAS_OK:
                        i += 1

                    # 4.1 - If reply is error, return the error to client
                    elif msg['body']['type'] == M_ERROR:
                        send(msg)
                        error = True
                        break
        
                
                # 5 - Return to client
                if(not error):
                    errorSimple(msg,type=M_CAS_OK)