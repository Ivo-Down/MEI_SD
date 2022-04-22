#!/usr/bin/env python3.9
# -*- coding: iso-8859-15 -*

#from asyncio.windows_events import NULL
#from ensurepip import version
from concurrent.futures import ThreadPoolExecutor
from distutils.command import check
from sqlite3 import Timestamp
import time
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



def sendRequestVoteRPC():
    for n in node_ids:
        if n != node_id:

            sendSimple(node_id, n, type=RPC_REQUEST_VOTE, 
            term = currentTerm,
            candidate_id = node_id,
            last_log_index = len(log),
            last_log_term = log[-1][0] )


def replicate_log():
    # Time elapsed since last replication
    global last_replication
    elapsed_time = time.time() - last_replication

    if leader and elapsed_time > MIN_REPLICATION_INTERVAL:
        # enough time has passed so leader is going to replicate its log
        logging.debug("Replicating log.")
        sendAppendEntriesRPC()
        last_replication = time.time()

    
    


def start_election():
    global leader, term_deadline
    if(not leader):
        # If it didn't receive RPC in some time, timesout and starts new election
        if(term_deadline < time.time()):
            become_candidate()
        # reset deadline
        else:
            reset_term_deadline()


def reset_term_deadline():
    global term_deadline
    term_deadline = time.time() + (TIMEOUT_INTERVAL * (random.random() + 1)) 


def reset_give_up_deadline():
    global candidate_deadline
    candidate_deadline = time.time() + ELECTION_TIMEOUT


def check_if_step_down(term):
    global currentTerm, voted_for
    if(currentTerm < term):
        # This leader is going to step down
        currentTerm = term
        voted_for = None
        become_follower()




def become_candidate():
    global currentTerm, voted_for, node_id, votes_gathered, leader
    logging.debug("Becoming candidate for term: " , currentTerm)
    # clears the list of votes
    leader = False
    votes_gathered = []
    currentTerm += 1
    voted_for = node_id
    reset_term_deadline()
    reset_give_up_deadline()
    request_votes()


def become_leader():
    global leader, last_replication

    leader = True
    last_replication = 0
    reset_give_up_deadline()
    # Creates 'nextIndex' | 'matchIndex' for each node, to keep track of their log state
    for n in node_ids:
        if n != node_id:
            nextIndex[n] = len(log) + 1
            matchIndex[n] = 0


def become_follower():
    global nextIndex, matchIndex, leader, currentTerm
    logging.debug("Becoming follower for term: " , currentTerm)
    nextIndex = None
    matchIndex = None
    leader = False
    reset_term_deadline()


def request_votes():
    global votes_gathered
    votes_gathered.append[node_id]
    # Broadcast vote request 
    sendRequestVoteRPC()



# Leader updates commit Index with the replies it has gotten
def update_commit_index():
    global matchIndex, commitIndex, log, currentTerm
    # We only consider an entry commited if it is majority stored
    # and at least one entry from leader's term is majority stored
    # median = int(sum(matchIndex.values()) / len(matchIndex))  #TODO n sei se funfa
    matchIndexValues = list(matchIndex.values())
    matchIndexValues.sort()
    size = len(matchIndexValues)
    median = size - int(math.floor((size / 2.0) + 1))

    # if commitIndex is bellow average and 
    if commitIndex < median and log[median][0] == currentTerm:
        commitIndex = median


# Leader applies one entry that is commited to the log
def update_state():
    logging.error('Updating state...\n')
    global lastApplied, commitIndex, log
    if lastApplied < commitIndex:
        lastApplied += 1
        operation_to_apply = log[lastApplied][1]
        res = apply_operation(operation_to_apply)

        # if it is the leader, reply to client
        if leader:
            msg_to_reply = operation_to_apply['request']
            if res['type'] == M_ERROR:
                errorSimple(msg_to_reply, type=M_ERROR, code=res['code'], text=res['text'])
            elif res['type'] == M_READ_OK:
                replySimple(msg_to_reply, type=M_READ_OK, value=res['value'])
            else:
                replySimple(msg_to_reply, type=res['type'])



def apply_operation(operation):
    if operation['type'] == 'read':
        key = operation['key']
        if key in dict:
            value = dict[key]
            return sn(type=M_READ_OK, value=value)
        else:
            #errorSimple(msg, type=M_ERROR, code=20, text='Key does not exist.')
            return sn(type=M_ERROR, code=20, text='Key does not exist.')

    elif operation['type'] == 'write':
        key = operation['key']
        value = operation['value']
        dict[key] = value
        return sn(type=M_WRITE_OK)
        
    elif operation['type'] == 'cas':
        key = operation['key']
        value_to = operation['value_to']
        value_from = operation['value_from']

        if(key in dict):
            real_value = dict.get(key)
            if(real_value == value_from):
                dict[key] = value_to
                return sn(type=M_CAS_OK)
            else:
                #errorSimple(msg, type=M_ERROR, code=22, text='From value does not match.') 
                return sn(type=M_ERROR, code=22, text='From value does not match.')
        else:
            #errorSimple(msg, type=M_ERROR, code=20, text='Key does not exist.')
            return sn(type=M_ERROR, code=20, text='Key does not exist.')






        
# - - - - - - - - - - - - - - - Main Cycle - - - - - - - - - - - - - - -



while True:
    #logging.info("IVOOOO for term: ")
    #logging.debug("IVOOOOb for term: ")
    #logging.log( logging.CRITICAL, "IVOOOOc for term: ")   #Nenhum destes está a printar


    msg = receive()
    # Send AppendEntriesRPC to other nodes
    replicate_log()
    #start_election()
    update_commit_index()
    update_state()

    if not msg:
        break

    if msg['body']['type'] == M_INIT:
        quorum_size, node_ids, node_id = handle_init(msg)
        currentTerm = 0
        first_log_entry = (0, None)
        log.append(first_log_entry)

        if node_id == node_ids[0]:  # We are fixing a leader to develop the 2 phase of the algo first 
            leader = True

            for x in node_ids[1:]:  #TODO VER ISTO MELHOR
                # send initial empty AppendEntries RPCs (heartbeat) to each server

                # initialize leader's structures
                nextIndex[x] = len(log) # last log index + 1
                matchIndex[x] = 0
                pass
            


    # Leader reads the value on its dictionary
    elif msg['body']['type'] == M_READ:
        if leader:
            key = msg['body']['key']
            operation = sn(type='read', key=key, request=msg)
            newLog = (currentTerm, operation)
            log.append(newLog)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')
        

    elif msg['body']['type'] == M_WRITE:
        if(leader):
            key = msg['body']['key']
            to_write = msg['body']['value']
            operation = sn(type='write', key=key, value=to_write, request=msg)
            newLog = (currentTerm, operation)
            log.append(newLog)
            # send to every follower the respective log entries, heartbeats
            #sendAppendEntriesRPC()
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')


    # Compares and sets 
    elif msg['body']['type'] == M_CAS:
        if(leader):
            key = msg['body']['key']
            value_from = msg['body']['from']
            value_to = msg['body']['to']
            operation = sn(type='cas', key=key, value_from=value_from , value_to=value_to, request=msg)
            newLog = (currentTerm, operation)
            log.append(newLog)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')






    elif msg['body']['type'] == RPC_APPEND_ENTRIES:
        term = msg['body']['term']
        prevLogIndexReceived = msg['body']['prevLogIndex']
        entriesReceived = msg['body']['entries']
        leaderCommit = msg['body']['leaderCommit']
        failed = False

        check_if_step_down(term)

        if(not leader):
            # 1. Reply false if term < currentTerm
            if term < currentTerm:
                failed = True
            else:
                # This leader's heartbeat is valid so we reset the timeout
                reset_term_deadline()

                # 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
                if log[prevLogIndexReceived]:
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
                        commitIndex = min(leaderCommit, len(log))
                    
                    # RPC_APPEND_OK
                    replySimple(msg, commitIndex=commitIndex, entries=log, term=currentTerm, type=RPC_APPEND_OK)

                else: 
                    replySimple(msg, term=currentTerm, type=RPC_APPEND_FALSE)

        





    elif msg['body']['type'] == RPC_APPEND_OK: 
        ci = msg['body']['commitIndex']
        entries = msg['body']['entries']
        term = msg['body']['term']
        n = msg['src']

        check_if_step_down(term) 
        if leader and term == currentTerm:   
            reset_give_up_deadline() 
            nextIndex[n] = max(nextIndex.get(n), ci+len(entries))
            matchIndex[n] = max(matchIndex.get(n), ci-1+len(entries))


    elif msg['body']['type'] == RPC_APPEND_FALSE:
        term = msg['body']['term']
        n = msg['src']

        check_if_step_down(term)
        if leader and term == currentTerm:
            reset_give_up_deadline()
            # Leader decrements one, so that he sends one more log.
            if(nextIndex.get(n) > 0):
                nextIndex[n] -= 1



    # Some node is requesting my vote
    elif msg['body']['type'] == RPC_REQUEST_VOTE:
        term = msg['body']['term']
        candidate_id = msg['body']['candidate_id']
        last_log_index = msg['body']['last_log_index']
        last_log_term = msg['body']['last_log_term']

        check_if_step_down(term)

        if (term < currentTerm):
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE)

        elif (voted_for == None or voted_for == candidate_id): # nao sei se aqui precisa do candidateid
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE)

        # if received log is in a lower term
        elif (last_log_term < log[-1][0]):
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE)

        # if logs are in the same term but received log is of smaller size
        elif (last_log_term == log[-1][0] and last_log_index < len(log)):
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE)

        # if it gets here, vote is granted
        else:
            voted_for = candidate_id
            reset_term_deadline()
            replySimple(msg, type=RPC_REQUEST_VOTE_OK)



    elif msg['body']['type'] == RPC_REQUEST_VOTE_OK:
        votes_gathered.append(msg['src'])
        
        # Check if majority of votes has been gathered
        majority = math.ceil((len(node_ids)+1)/2)
        if(len(votes_gathered) >= majority):
            become_leader()




    else:
        logging.warning('Unknown message type %s', msg['body']['type'])
