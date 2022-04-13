#!/usr/bin/env python3.9
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
from concurrent.futures import ThreadPoolExecutor
from sqlite3 import Timestamp
from time import time
from ms import *

import logging
from commands import *
from constants import *
from types import SimpleNamespace as sn



# - - - Auxiliar Functions - - -
# build and send a AppendEntriesRPC (this is only sent by the leader)
def sendAppendEntriesRPC():

    for n in node_ids:
        if n != node_id:

            prevLogIndex = nextIndex.get(n) - 1
            prevLogTerm = log[nextIndex.get(n) - 1][0]

            sendSimple(node_id, n, type=RPC_APPEND_ENTRIES, 
            term = currentTerm,
            prevLogIndex = prevLogIndex,
            prevLogTerm = prevLogTerm,
            entries = log,
            leaderCommit = commitIndex )

                    
def replicate_log():

    # Time elapsed since last replication
    elapsed_time = time.time() - last_replication

    if elapsed_time > MIN_REPLICATION_INTERVAL:
        sendAppendEntriesRPC()
        last_replication = time.time()
    






logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dict = {}
# - - - Persistant state on all servers - - -
leader = False
currentTerm = -1    # leader's term (-1 for unitialized)
log = []            # log entries to store (empty for heartbeat; may send more than one for efficiency)
                    # entries will be pairs of (currentTerm, operation)
voted_for = None    # candidate that received the vote in the current term
last_replication = 0# time in seconds since last replication


# - - - Volatile state on all servers - - -
commitIndex = 0     # index of highest log entry known to be
                    # committed (initialized to 0, increases monotonically)

lastApplied = 0     # index of highest log entry applied to state
                    # machine (initialized to 0, increases monotonically)


# - - - Volatile state on leader - - -
nextIndex = {}      # for each server, index of the next log entry to send to that server 
                    # (initialized to leader last log index + 1)
                    # key: nodeId   value: nextIndex of that node

matchIndex = {}     # for each server, index of highest log entry known to be replicated on server
                    # (initialized to 0, increases monotonically)
                    # key: nodeId   value: matchIndex of that node


#leaderId => atm not necessary
# comitIndex: valor pela qual já existe uma maioria de logs que o tem
# Esse commit iindex, na prox appendEntries vai ser enviado
# Só se "executa" entre o lastApplieed e o commit index

while True:

    msg = receive()
    # Send AppendEntriesRPC to other nodes
    if (leader):
        sendAppendEntriesRPC()


    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        quorum_size, node_ids, node_id = handle_init(msg)
        currentTerm = 0
        first_log_entry = (0, None)
        log.append(first_log_entry)

        if node_id == node_ids[0]:  # We are fixing a leader to develop the 2 phase of the algo first 
            leader = True

            for x in node_ids[1:]:
                # send initial empty AppendEntries RPCs (heartbeat) to each server

                # initialize leader's structures
                nextIndex[x] = len(log) # last log index + 1
                matchIndex[x] = 0
                pass
            


    # Leader reads the value on its dictionary
    elif msg['body']['type'] == M_READ:
        if leader:
            key = msg['body']['key']
            if key in dict:
                value = dict[key]
                replySimple(msg, type=M_READ_OK, value=value)
            else:
                errorSimple(msg, type=M_ERROR, code=20, text='Key does not exist.')
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')
        

    elif msg['body']['type'] == M_WRITE:
        if(leader):
            key = msg['body']['key']
            to_write = msg['body']['value']
            operation = sn(type = 'write', key = key, value = to_write)

            # save new command in the logs
            newLog = (currentTerm, operation)
            log.append(newLog)
            
            # send to every follower the respective log entries, heartbeats
            sendAppendEntriesRPC()

            # write on leader dictionary after receiving majority of acks
            dict[key] = to_write

            # reply to the client
            replySimple(msg, type=M_WRITE_OK)

        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        if(leader):
            key = msg['body']['key']
            value_from = msg['body']['from']
            value_to = msg['body']['to']
            
            if(key in dict):
                real_value = dict.get(key)

                # In this case it will update the key's value
                if(real_value == value_from):
                    # save new command in the logs
                    operation = sn(type = 'cas', key = key, value = value_to)
                    newLog = (currentTerm, operation)
                    log.append(newLog)
                    
                    # send to every follower the respective log entries, this could be
                    # added to a queue and sent in hearthbeats
                    sendAppendEntriesRPC()

                    
                    # write on leader dictionary after receiving majority of acks
                    dict[key] = value_to

                    # reply to the client
                    replySimple(msg, type=M_CAS_OK)

                else:
                    errorSimple(msg, type=M_ERROR, code=22, text='From value does not match.')

                
            else:
                errorSimple(msg, type=M_ERROR, code=20, text='Key does not exist.')

        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    elif msg['body']['type'] == RPC_APPEND_ENTRIES:
        if(not leader):
            term = msg['body']['term']
            prevLogIndexReceived = msg['body']['prevLogIndex']
            entriesReceived = msg['body']['entries']
            leaderCommit = msg['body']['leaderCommit']
            failed = False
            
            # 1. Reply false if term < currentTerm
            if term < currentTerm:
                failed = True
            else:
                # 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
                if log[prevLogIndexReceived]:
                    # If an existing entry conflicts with a new one (same index but different terms),
                    if log[prevLogIndexReceived][0] != term:
                        # delete the existing entry and all that follows it
                        log = log[:prevLogIndexReceived]
                    else:
                        failed = True
                else:
                    failed = True

                if not failed:
                    # 4. Append any new entries not already in the log
                    log.extend(entriesReceived)
                    
                    # 5. If leaderCommit > commitIndex, set commitIndex =
                    # min(leaderCommit, index of last new entry)
                    if (leaderCommit > commitIndex):
                        commitIndex = min(leaderCommit, len(log) - 1)
                    
                    # RPC_APPEND_OK
                    replySimple(msg, commitIndex=commitIndex, entries=log, type=RPC_APPEND_OK)

                else: 
                    replySimple(msg, type=RPC_APPEND_FALSE)

    elif msg['body']['type'] == RPC_APPEND_OK:
        
        if leader:

            ci = msg['body']['commitIndex']
            entries = msg['body']['entries']
            n = msg['src']

            nextIndex[n] = max(nextIndex.get(n), ci+len(entries))
            matchIndex[n] = max(matchIndex.get(n), ci-1+len(entries))

    elif msg['body']['type'] == RPC_APPEND_FALSE:
        if leader:
            n = msg['src']
            # Leader decrements one, so that he sends one more log.
            if(nextIndex.get(n) > 0):
                nextIndex[n] -= 1

    else:
        logging.warning('Unknown message type %s', msg['body']['type'])






# Main loop
#executor.map(lambda msg: exitOnError(handle, msg), receiveAll())


# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()