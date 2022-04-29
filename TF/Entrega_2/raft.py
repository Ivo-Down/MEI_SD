#!/usr/bin/env python3.9
# -*- coding: iso-8859-15 -*

from pickle import TRUE
from sqlite3 import Timestamp
import time
from ms import *
import logging
from commands import *
from constants import *
from types import SimpleNamespace as sn
import math
import random



# - - - Persistant state on all servers - - -
leader = False
candidate = False
state = {}


currentTerm = 0     # leader's term (0 for unitialized)

log = [(0, None)]   # log entries to store (empty for heartbeat; may send more than one for efficiency)
                    # entries will be pairs of (currentTerm, operation)


# - - - Volatile state on all servers - - -
commitIndex = 0     # index of highest log entry known to be
                    # committed (initialized to 0, increases monotonically)   

lastApplied = 0     # index of highest log entry applied to state
                    # machine (initialized to 0, increases monotonically)

# comitIndex: valor pela qual já existe uma maioria de logs que o tem
# Esse commit iindex, na prox appendEntries vai ser enviado
# Só se "executa" entre o lastApplieed e o commit index


# - - - Volatile state on leader - - -
nextIndex = {}      # for each server, index of the next log entry to send to that server 
                    # (initialized to leader last log index + 1)
                    # key: nodeId   value: nextIndex of that node

matchIndex = {}     # for each server, index of highest log entry known to be replicated on server
                    # (initialized to 0, increases monotonically)
                    # key: nodeId   value: matchIndex of that node



# - - - Election Variables - - -

lastReplication = 0 # time in seconds since last replication

votedFor = None    # candidate that received the vote in the current term

votesGathered = [] # list of all votes obtained to become leader for current term

electionTimer =  0 # when to start the next election, in seconds 

giveUpTimer = 0 # when to give up becoming a leader





# - - - - - - - - - - - - - - - Replication Functions - - - - - - - - - - - - - - -

# build and send a AppendEntriesRPC (this is only sent by the leader)
def sendAppendEntriesRPC():
    global node_ids, node_id, log, nextIndex, commitIndex, currentTerm
    for n in node_ids:
        if n != node_id:
            ni = nextIndex[n]
            entries_to_send = log[ni - 1:]

            if len(entries_to_send) > 0:

                prevLogIndex = ni - 1
                prevLogTerm = log[ni - 2][0]

            
                sendSimple(node_id, n, type=RPC_APPEND_ENTRIES, 
                term = currentTerm,
                prevLogIndex = prevLogIndex,
                prevLogTerm = prevLogTerm,
                entries = entries_to_send,
                leaderCommit = commitIndex )


def replicateLog():
    # Time elapsed since last replication
    global lastReplication
    elapsed_time = time.time() - lastReplication

    if leader and elapsed_time > MIN_REPLICATION_INTERVAL:                  
        # enough time has passed so leader is going to replicate its log
        sendAppendEntriesRPC() # functions as heartbeat as well
        lastReplication = time.time()
    

# Leader updates commit Index with the replies it has gotten
def updateCommitIndex():
    global matchIndex, commitIndex, log, currentTerm, leader
    if leader:
        # We only consider an entry commited if it is majority stored
        # and at least one entry from leader's term is majority stored

        matchIndexValues = list(matchIndex.values())
        matchIndexValues.sort()
        size = len(matchIndexValues)
        median_pos = size - int(math.floor((size / 2.0) + 1))
        median = matchIndexValues[median_pos]

        # if commitIndex is bellow average and 
        if commitIndex < median and log[median - 1][0] == currentTerm:
            #logging.info("Updating commitIndex to ", median)
            commitIndex = median


# Leader applies one entry that is commited to the log
def updateState():
    global lastApplied, commitIndex, log

    if lastApplied < commitIndex:
        lastApplied += 1
        operation_to_apply = log[lastApplied - 1][1]
        applyOperation(operation_to_apply)


# Replies to client if it is the leader
def applyOperation(operation):
    global leader, state
    if operation['body']['type'] == M_READ:
        key = operation['body']['key']
        if key in state:
            value = state[key]
            if leader:
              replySimple(operation, type=M_READ_OK, value=value)
        else:
            if leader:
                errorSimple(operation, type=M_ERROR, code=20, text='Key does not exist.')

    elif operation['body']['type'] == M_WRITE:
        key = operation['body']['key']
        value = operation['body']['value']
        state[key] = value
        if leader:
          replySimple(operation, type=M_WRITE_OK)
        
    elif operation['body']['type'] == M_CAS:
        key = operation['body']['key']
        value_to = operation['body']['to']
        value_from = operation['body']['from']

        if(key in state):
            real_value = state.get(key)
            if(real_value == value_from):
                state[key] = value_to
                if leader:
                  replySimple(operation, type=M_CAS_OK)
            else:
                if leader:
                    errorSimple(operation, type=M_ERROR, code=22, text='From value does not match.') 
        else:
            if leader:
                errorSimple(operation, type=M_ERROR, code=20, text='Key does not exist.')
    




# - - - - - - - - - - - - - - - Election Functions - - - - - - - - - - - - - - -

# build and send a RequestVoteRPC 
def sendRequestVoteRPC():
    global node_ids, node_id, log, currentTerm
    for n in node_ids:
        if n != node_id:

            sendSimple(node_id, n, type=RPC_REQUEST_VOTE, 
            term = currentTerm,
            candidate_id = node_id,
            last_log_index = len(log),
            last_log_term = log[-1][0] )

    
# If the node timesOut it starts a new election phase
def startElection():
    global leader, candidate, electionTimer
    if time.time() > electionTimer:
        # If it didn't receive RPC in some time, timesout and starts new election if not leader yet
        if not leader:
            changeToCandidate()
        else:
            resetElectionTimer()


# Defines time until next election will be triggered if no messages come, using a random factor
def resetElectionTimer():
    global electionTimer
    electionTimer = time.time() + (ELECTION_TIMEOUT * (random.random() + 1)) 


def resetGiveUpTimer():
    global giveUpTimer
    giveUpTimer = time.time() + ELECTION_TIMEOUT


# If the term given is bigger than the currentTerm, leader changes to a follower
def checkGiveUp(term):
    global currentTerm, votedFor
    if currentTerm < term:
        # This leader is going to step down
        currentTerm = term
        votedFor = None
        changeToFollower()


# If leader has not received any acks for a while, times out
def stepDownOnTimeout():
    global leader, giveUpTimer
    if (leader and time.time() > giveUpTimer):
       changeToFollower()
    pass


def changeToCandidate():
    global currentTerm, votedFor, node_id, votesGathered, leader, candidate
    #logging.debug("Becoming candidate for term: " , currentTerm, node_id)
    # clears the list of votes
    leader = False
    candidate = True
    votedFor = node_id
    votesGathered = []
    currentTerm += 1
    resetElectionTimer()
    resetGiveUpTimer()
    requestVotes()


def changeToLeader():
    global leader, candidate, lastReplication, nextIndex, matchIndex, log, currentTerm
    if candidate:
        #logging.debug("Becoming leader for term: " , currentTerm)
        leader = True
        candidate = False
        lastReplication = 0
        # Creates 'nextIndex' | 'matchIndex' for each node, to keep track of their log state
        for n in node_ids:
            if n != node_id:
                nextIndex[n] = len(log) + 1   
                matchIndex[n] = 0
        resetGiveUpTimer()


def changeToFollower():
    global nextIndex, matchIndex, leader, candidate, currentTerm, votedFor, votesGathered
    #logging.debug("Becoming follower for term: " , currentTerm)
    nextIndex = {}
    matchIndex = {}
    leader = False
    candidate = False
    votedFor = None
    votesGathered = []
    resetElectionTimer()


def requestVotes():
    global votesGathered, node_id
    votesGathered.append(node_id)  #adds its own vote
    # Broadcast vote request 
    sendRequestVoteRPC()




        
# - - - - - - - - - - - - - - - Main Cycle - - - - - - - - - - - - - - -

# Handles messages that arrive to the node
def mainLoop():
    msg = receive()
    global node_id, node_ids, currentTerm, lastApplied, log, leader, nextIndex, matchIndex, commitIndex, votedFor
    
    if not msg:
        logging.debug("NO MESSAGE!!")
        return None
    

    if msg['body']['type'] == M_INIT:
        node_ids, node_id = handle_init(msg)
        currentTerm = 0
        lastApplied = 1
        #first_log_entry = (0, None)
        #log.append(first_log_entry)
        changeToFollower()
            

    elif msg['body']['type'] == M_READ or msg['body']['type'] == M_WRITE or msg['body']['type'] == M_CAS:
        if leader:
            newLog = (currentTerm, msg)
            log.append(newLog)
        else:
            errorSimple(msg, type=M_ERROR, code=11, text='Not the leader.')
        

    elif msg['body']['type'] == RPC_APPEND_ENTRIES:
        prevLogTerm = msg['body']['prevLogTerm']
        prevLogIndexReceived = msg['body']['prevLogIndex']
        entriesReceived = msg['body']['entries']
        leaderCommit = msg['body']['leaderCommit']
        failed = False
        term = msg['body']['term']

        checkGiveUp(term)
        
        # 1. Reply false if term < currentTerm
        if term < currentTerm:
            failed = True
            
        else:
            # There is someone with a higher term so they probably are the leader
            resetElectionTimer()

            if prevLogIndexReceived <= 0:
                failed = True
            
            else:
                # 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
                if prevLogIndexReceived - 1 < len(log): #check if index exists 
                    # 3. If an existing entry conflicts with a new one (same index but different terms),
                    if log[prevLogIndexReceived - 1][0] != prevLogTerm:
                        failed = True

                    else:
                        # 3. delete the existing entry and all that follows it
                        log = log[0:prevLogIndexReceived] 
                        # 4. Append any new entries not already in the log 
                        log.extend(entriesReceived)

                        # 5. If leaderCommit > commitIndex, set commitIndex =
                        # min(leaderCommit, index of last new entry)
                        if (leaderCommit > commitIndex):
                            commitIndex = min(leaderCommit, len(log))  

                else:
                    failed = True

        if failed:
            replySimple(msg, term=currentTerm, type=RPC_APPEND_FALSE)
        else: 
            replySimple(msg, term=currentTerm, type=RPC_APPEND_OK)

            
  
    elif msg['body']['type'] == RPC_APPEND_OK: 
        term = msg['body']['term']
        n = msg['src']
        checkGiveUp(term)

        if leader and term == currentTerm:
            ni = nextIndex[n]
            entries = log[ni - 1:]
            resetGiveUpTimer()   
            nextIndex[n] = max(ni, ni+len(entries))  # é um bocado redundante isto
            matchIndex[n] = max(matchIndex.get(n), ni+len(entries)-1)


    elif msg['body']['type'] == RPC_APPEND_FALSE:
        term = msg['body']['term']
        n = msg['src']
        checkGiveUp(term)

        if leader and term == currentTerm:
            resetGiveUpTimer()
            # Leader decrements one, so that it sends one more log.
            nextIndex[n] -= 1


    # Some node is requesting my vote
    elif msg['body']['type'] == RPC_REQUEST_VOTE:
        term = msg['body']['term']
        candidate_id = msg['body']['candidate_id']
        last_log_index = msg['body']['last_log_index']
        last_log_term = msg['body']['last_log_term']

        checkGiveUp(term)

        if (term < currentTerm): # candidate has a lower term than this node's
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE, term=currentTerm)

        elif (votedFor != None ): # if it has already voted
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE, term=currentTerm)

        elif (last_log_term < log[-1][0]): # if received log is in a lower term
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE, term=currentTerm)

        # if logs are in the same term but received log is of smaller size
        elif (last_log_term == log[-1][0] and last_log_index < len(log)):
            replySimple(msg, type=RPC_REQUEST_VOTE_FALSE, term=currentTerm)

        # if it gets here, vote is granted
        else:
            votedFor = candidate_id
            resetElectionTimer()
            replySimple(msg, type=RPC_REQUEST_VOTE_OK, term=currentTerm)


    elif msg['body']['type'] == RPC_REQUEST_VOTE_OK:
        term = msg['body']['term']
        resetGiveUpTimer()
        checkGiveUp(term)
        
        if(currentTerm == term and candidate):
            votesGathered.append(msg['src'])
            # Check if majority of votes has been gathered
            
            majority = int(math.floor((len(node_ids) / 2.0) + 1))
            if(majority <= len(votesGathered)):
                changeToLeader()


    elif msg['body']['type'] == RPC_REQUEST_VOTE_FALSE:
        term = msg['body']['term']
        resetGiveUpTimer()
        checkGiveUp(term)




# Main cycle where constantly tries to receive message, check if it should timeout,
# tries to replicateLog if leader, tries to begin a new election if enough time has passed,
# and tries to update commitIndex as well as the state if it's the leader.
def main():
    while True:
        try:
            mainLoop() or stepDownOnTimeout() or replicateLog() or \
            startElection() or updateCommitIndex() or updateState() or \
            time.sleep(0.002)
        except:
            logging.debug("AN ERROR HAS OCCURRED!!")
            break


if __name__ == '__main__':
    main()
