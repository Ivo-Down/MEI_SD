#!/usr/bin/env python3.9
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
from concurrent.futures import ThreadPoolExecutor
from distutils.command import check
from sqlite3 import Timestamp
import time
import traceback
from ms import *
import logging
from commands import *
from constants import *
from types import SimpleNamespace as sn
import random



logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dict = {}
# - - - Persistant state on all servers - - -
leader = False
currentTerm = 0    # leader's term (-1 for unitialized)
log = []            # log entries to store (empty for heartbeat; may send more than one for efficiency)
                    # entries will be pairs of (currentTerm, operation)
voted_for = None    # candidate that received the vote in the current term

last_replication = 0# time in seconds since last replication
term_deadline = 0   # time in seconds since last RPC received
candidate_deadline = 0 # when to give up becoming a leader


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

votes_gathered = [] # list of all votes obtained to become leader for current term


#leaderId => atm not necessary
# comitIndex: valor pela qual já existe uma maioria de logs que o tem
# Esse commit iindex, na prox appendEntries vai ser enviado
# Só se "executa" entre o lastApplieed e o commit index



# - - - - - - - - - - - - - - - Auxiliar Functions - - - - - - - - - - - - - - -

# build and send a AppendEntriesRPC (this is only sent by the leader)
def sendAppendEntriesRPC():
    for n in node_ids:
        if n != node_id:
            entries_to_send = log[nextIndex.get(n)-1:]

            prevLogIndex = nextIndex.get(n) - 1
            prevLogTerm = log[nextIndex.get(n) - 1][0]

            

            sendSimple(node_id, n, type=RPC_APPEND_ENTRIES, 
            term = currentTerm,
            prevLogIndex = prevLogIndex,
            prevLogTerm = prevLogTerm,
            entries = entries_to_send,
            leaderCommit = commitIndex )




def replicate_log():
    # Time elapsed since last replication
    global last_replication
    elapsed_time = time.time() - last_replication

    if leader and elapsed_time > MIN_REPLICATION_INTERVAL:                  
        # enough time has passed so leader is going to replicate its log
        last_replication = time.time()
        sendAppendEntriesRPC()

    
    




# Leader updates commit Index with the replies it has gotten
def update_commit_index():
    if leader:
        global matchIndex, commitIndex, log, currentTerm
        # We only consider an entry commited if it is majority stored
        # and at least one entry from leader's term is majority stored

        # median = int(sum(matchIndex.values()) / len(matchIndex))  #TODO n sei se funfa

        matchIndexValues = list(matchIndex.values())
        matchIndexValues.sort()
        size = len(matchIndexValues)
        median_pos = size - int(math.floor((size / 2.0) + 1))
        median = matchIndexValues[median_pos]

        # if commitIndex is bellow average and 
        if commitIndex < median and log[median][0] == currentTerm:
            logging.info("Updating commitIndex to ", median)
            commitIndex = median


# Leader applies one entry that is commited to the log
def update_state():
    global lastApplied, commitIndex, log

    if lastApplied < commitIndex:
        lastApplied += 1
        operation_to_apply = log[lastApplied][1]
        apply_operation(operation_to_apply)


# Replies to client if it is the leader
def apply_operation(operation):
    if operation['body']['type'] == 'read':
        key = operation['body']['key']
        if key in dict:
            value = dict[key]
            if leader:
              replySimple(operation, type=M_READ_OK)
        else:
            if leader:
                errorSimple(operation, type=M_ERROR, code=20, text='Key does not exist.')

    elif operation['body']['type'] == 'write':
        key = operation['body']['key']
        value = operation['body']['value']
        dict[key] = value
        if leader:
          replySimple(operation, type=M_WRITE_OK)
        
    elif operation['body']['type'] == 'cas':
        key = operation['body']['key']
        value_to = operation['body']['to']
        value_from = operation['body']['from']

        if(key in dict):
            real_value = dict.get(key)
            if(real_value == value_from):
                dict[key] = value_to
                if leader:
                  replySimple(operation, type=M_CAS_OK)
            else:
                if leader:
                    errorSimple(operation, type=M_ERROR, code=22, text='From value does not match.') 
        else:
            if leader:
                errorSimple(operation, type=M_ERROR, code=20, text='Key does not exist.')
    






        
# - - - - - - - - - - - - - - - Main Cycle - - - - - - - - - - - - - - -

def main_loop():
    msg = receive()

    global node_id, node_ids, 
    
    if not msg:
        #logging.debug("NO MESSAGE!!")
        pass
    
    if msg['body']['type'] == M_INIT:
        node_ids, node_id = handle_init(msg)
        currentTerm = 0
        first_log_entry = (0, None)
        log.append(first_log_entry)

        if node_id == node_ids[0]:  # We are fixing a leader to develop the 2 phase of the algo first 
            leader = True

            for x in node_ids[1:]:  #TODO VER ISTO MELHOR
                # initialize leader's structures
                nextIndex[x] = len(log) # last log index + 1
                matchIndex[x] = 0
            

    elif msg['body']['type'] == M_READ:
        if leader:
            key = msg['body']['key']
            newLog = (currentTerm, msg)
            log.append(newLog)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')
        

    elif msg['body']['type'] == M_WRITE:
        if(leader):
            key = msg['body']['key']
            to_write = msg['body']['value']
            newLog = (currentTerm, msg)
            log.append(newLog)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    elif msg['body']['type'] == M_CAS:
        if(leader):
            key = msg['body']['key']
            value_from = msg['body']['from']
            value_to = msg['body']['to']
            newLog = (currentTerm, msg)
            log.append(newLog)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    elif msg['body']['type'] == RPC_APPEND_ENTRIES:
        term = msg['body']['term']
        prevLogIndexReceived = msg['body']['prevLogIndex']
        entriesReceived = msg['body']['entries']
        leaderCommit = msg['body']['leaderCommit']
        failed = False
        
        if(not leader):
            # 1. Reply false if term < currentTerm
            if term < currentTerm:
                failed = True
            else:
                # 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
                if prevLogIndexReceived < len(log): #check if index exists
                    # 3. If an existing entry conflicts with a new one (same index but different terms),
                    if log[prevLogIndexReceived][0] == term:
                        # 3. delete the existing entry and all that follows it
                        log = log[0:prevLogIndexReceived]

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
                    replySimple(msg, term=currentTerm, type=RPC_APPEND_OK)

                else: 
                    replySimple(msg, term=currentTerm, type=RPC_APPEND_FALSE)

  
    elif msg['body']['type'] == RPC_APPEND_OK: 
        term = msg['body']['term']
        n = msg['src']
        ni = nextIndex[n]
        entries = log[ni:]   #TODO mudei aquiiiii

        if leader and term == currentTerm:   
            nextIndex[n] = max(ni, ni+len(entries))  # é um bocado redundante isto
            matchIndex[n] = max(matchIndex.get(n), ni+len(entries)-1)


    elif msg['body']['type'] == RPC_APPEND_FALSE:
        term = msg['body']['term']
        n = msg['src']

        if leader and term == currentTerm:
            # Leader decrements one, so that it sends one more log.
            #if(nextIndex.get(n) > 0):
            nextIndex[n] -= 1


    else:
        logging.warning('Unknown message type %s', msg['body']['type'])


def main():
    while True:
        try:
            main_loop() or \
            replicate_log() or \
            update_commit_index() or \
            update_state() or \
            time.sleep(0.001)
        except KeyboardInterrupt:
            logging.error("Aborted by interrupt!")
            break
        except:
            logging.error("Error!", traceback.format_exc())

main()