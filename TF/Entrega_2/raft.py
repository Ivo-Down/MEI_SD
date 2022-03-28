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
from types import SimpleNamespace as sn



# - - - Auxiliar Functions - - -
# build and send a AppendEntriesRPC (this is only sent by the leader)
def sendAppendEntriesRPC(term, leaderId, prevLogIndex, prevLogTerm, leaderCommitIndex, entries[]): #TODO
    pass



logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)
dict = {}
leader = False
currentTerm = -1    # leader's term (-1 for unitialized)
prevLogIndex = -1   # index of log entry immediately preceding new ones (-1 for unitialized)
prevLogTerm = -1    # term of prevLogIndex entry (-1 for unitialized)
leaderCommits = []  # leader’s commitIndex for each node

entries = []        #log entries to store (empty for heartbeat; may send more than one for efficiency)
                    # entries will be pairs of (currentTerm, operation)

#leaderId => atm not necessary

# comitIndex: valor pela qual já existe uma maioria de logs que o tem
# Esse commit iindex, na prox appendEntries vai ser enviado
# Só se "executa" entre o lastApplieed e o commit index

while True:
    msg = receive()
    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        quorum_size, node_ids, node_id = handle_init(msg)
        if node_id == node_ids[0]:  # We are fixing a leader to develop the 2 phase of the algo first
            currentTerm = 1
            leader = True
            """
            for x in node_ids[1:]:
                # send initial empty AppendEntries RPCs (heartbeat) to each server
                pass
            """


    # Leader reads the value on its dictionary
    elif msg['body']['type'] == M_READ:
        if leader:
            key = msg['body']['key']
            value = dict[key]
            replySimple(msg, type=M_READ_OK, value=value)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')
        

    elif msg['body']['type'] == M_WRITE:
        if(leader):
            key = msg['body']['key']
            to_write = msg['body']['value']

            # write on leader dictionary
            dict[key] = to_write
            entries.append(sn(type = M_WRITE, key = key, value = to_write)) 
            
            # send to every follower the respective log entries, heartbeats
            #TODO POR ISTO COM PROG POR EVENTOS
            for n in node_ids:
                if n != node_id:
                    sendAppendEntriesRPC(
                        currentTerm, node_id, prevLogIndex,
                         prevLogTerm, leaderCommits[n], entries)

            # reply to the client
            replySimple(msg, type=M_WRITE_OK)

        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        if(leader):
            #do things
            pass
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    elif msg['body']['type'] == RPC_APPEND_ENTRIES:  #TODO
        if(not leader):
            term = msg['body']['term']
            pass
    """
    1. false if term < currentTerm
    2. false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    3. If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it
    4. Append any new entries not already in the log
    5.If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of last new entry)
    
    """


    else:
        logging.warning('Unknown message type %s', msg['body']['type'])






# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())


# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()