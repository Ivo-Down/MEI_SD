import random
from constants import *
from ms import *

def choose_quorum(list, quorum_size):
    return random.sample(list, quorum_size)


def get_locks(msg,quorum_to_use, node_id):
    for node in quorum_to_use:
        sendSimple(node_id,node,type=QR_LOCK)



# If the lock replies are all in, checks to see if it failed to get some lock and if it did it releases all the others
def check_lock_state(node_locks, node_id, quorum_size, msg, quorum_to_use):  
    if len(node_locks.keys()) == quorum_size: #key -> nodo  value -> lock state
        if False in node_locks.values():
            for k,v in node_locks.items():
                if v:
                    # If the node was locked, send an unlock request
                    sendSimple(node_id, k, type=QR_UNLOCK, text = 'Release lock request')
                    errorSimple(msg, type=M_ERROR, code=11, text = 'Read quorum not available')

        # If it got every lock, procceeds
        else:
            key = msg['body']['key']
            # 2 - Collects pairs (value, timestamp) from each node of the qr
            for node in quorum_to_use:
                sendSimple(node_id, node, type=QR_READ, key=key)