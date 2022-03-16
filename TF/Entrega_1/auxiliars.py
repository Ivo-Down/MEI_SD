import random
from constants import *
from ms import *

def choose_quorum(list, node, quorum_size):
    if(node in list):
        list.remove(node)
    return random.sample(list, quorum_size)


def get_locks(msg,quorum_to_use, node_id):
    global next_id
    failed_locks = []
    for node in quorum_to_use:
        sendSimple(node_id,node,type=QR_LOCK)
    
    for node in quorum_to_use:
        msg_aux = receive()

        if not msg_aux:
            break

        elif msg_aux['body']['type'] == QR_LOCK_FAIL:
            failed_locks.append(node)

    if(failed_locks):  #If it failed to get a lock, unlock all the other nodes that were locked and returns false
        errorSimple(msg,type=M_ERROR,code=11,text='Read quorum not available (lock problem)')
        for lock in list(set(quorum_to_use) - set(failed_locks)):
            sendSimple(node_id,lock,type=QR_UNLOCK,text='Release lock request')
        return False
        
    else: 
        return True
