#!/usr/bin/env python

import sys
import socket
import json
import SocketServer
import subprocess
import threading
import os
import time

import random

import scout
import commander

LOCK_SUCCESS = 0
LOCK_FAILURE = 1
LOCK_WAIT = 3

UNLOCK_SUCCESS = 0
UNLOCK_FAILURE = 1

backlog = 5
maxbuf = 10240

backoff_lower_bound = 10
backoff_upper_bound = 15

active_leader_timeout_threshold = 10
send_keepalive_period = 8
leader_listen_timeout = 5

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

def pprint(msg):
    print json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))


class Leader:
    def __init__(self, leader_id):
        # network state
        self.leader_address = tuple(paxos_config["leaders"][leader_id])
        self.leader_id = leader_id

        # Paxos state
        self.active = False
        self.leader_ballot_num = (0, self.leader_id)
        self.proposals = []
        self.last_heard_leader_time = time.time()
        self.last_send_keepalive = time.time()
        self.other_leader_ids = paxos_config["leaders"].keys()
        self.other_leader_ids.remove(self.leader_id)

    def send_out_scout(self):
        # increment leader_ballot_num
        self.leader_ballot_num = (self.leader_ballot_num[0] + 1, self.leader_id)
        my_scout = scout.Scout(self.leader_id, self.leader_id, self.leader_ballot_num)
        print "Scout # " + self.leader_id + " started at " + str(my_scout.scout_address)
        my_scout.daemon = True
        my_scout.start()

    def spawn_scouts_and_commanders(self):
        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.leader_address)
        s.listen(backlog)
        s.settimeout(leader_listen_timeout)

        # spawn scout to execute p1 phase
        # scout sends back "adopted" if quorum of acceptors prepares ok
        # scout sends back "preempted" 
        # if scout finds an acceptor who has prepares to higher ballot_num
        self.send_out_scout()

        # event loop
        while 1:
            # check time and send out periodic keepalive
            if self.active:
                self.check_time_and_send_keepalive()
            else:
                # check the time since last heard from active leader
                # if current leader is inactive
                self.check_time_since_last_heard_leader()
            
            try:
                print "listening for connection"            
                # listen for minion p1b response
                minion_conn, minion_address = s.accept()
                minion_conn.settimeout(None)
                data = minion_conn.recv(maxbuf).strip()
                if data:
                    msg = json.loads(data)
                    if msg["type"] == "propose":
                        slot_num = msg["slot_num"]
                        proposal_value = msg["proposal_value"]
                        print "RECV propose msg from replica "
                        pprint(msg)
                        # if proposal slot num is already assigned
                        # or if proposal value is already proposed for a different slot_num
                        # discard proposal
                        found_duplicate = False
                        for p in self.proposals:
                            if slot_num == p["slot_num"] or proposal_value == p["proposal_value"]:
                                print "proposal discarded since the slot_num has already been assigned to another proposal_value"
                                found_duplicate = True
                        if found_duplicate:
                            continue

                        # it is indeed a new proposal        
                        print "proposal assigned to a new slot_num " + str(slot_num)
                        new_proposal = {"slot_num" : slot_num,
                                        "proposal_value" : proposal_value
                                        }
                        self.proposals += [new_proposal]
                        
                        # check the time since last heard from active leader
                        # if current leader is inactive
                        self.check_time_since_last_heard_leader()
                        
                        # if leader thinks it's active, goes to p2 phase
                        # spawn a commander to execute p2 phase
                        # commander sends back "pre-empted" if a true leader is active
                        # commander sends "decision" to all replicas
                        # if commander recv quorum of acceptor p2b messages with same ballot_num
                        if self.active:
                            proposal = {"ballot_num" : self.leader_ballot_num,
                                        "slot_num" : slot_num,
                                        "proposal_value" : proposal_value}
                            my_commander = commander.Commander(self.leader_id, self.leader_id, proposal)
                            my_commander.daemon = True
                            my_commander.start()

                    elif msg["type"] == "adopted":
                        print "leader recv adopted message"
                        self.active = True
                        print "leader become active"
                        pprint(msg)
                        accepted_proposals = msg["accepted_proposals"]
                        # retain proposals of highest ballot number
                        extracted_proposals = self.extracted_proposals_of_highest_ballot_number(accepted_proposals)
                        # union with accepted_proposals
                        # allow leader's current proposals to be overwritten by higher ballot numbered proposals
                        self.update_proposals_with_extracted_proposals(extracted_proposals)
                        
                        # spawn commander for each proposal
                        for proposal in self.proposals:
                            proposal_with_leader_ballot_num = dict(proposal)
                            proposal_with_leader_ballot_num["ballot_num"] = self.leader_ballot_num
                            my_commander = commander.Commander(self.leader_id, self.leader_id, 
                                                    proposal_with_leader_ballot_num)
                            print "leader # " + self.leader_id + "spawning commander for proposal " + str(proposal_with_leader_ballot_num)
                            my_commander.daemon = True
                            my_commander.start()

                    elif msg["type"] == "preempted":
                        if tuple(msg["ballot_num"]) > self.leader_ballot_num:
                            self.active = False
                            print "leader become inactive"
                            pprint(msg)

                            # # inactive leader start monitoring active leader
                            # active_leader_id = tuple(msg["ballot_num"])[1]
                            # print "starting ping active leader # " + str(active_leader_id)
                            # if self.ping_active_leader_timeout(active_leader_id):
                            #     # increment leader_ballot_num
                            #     self.leader_ballot_num = (self.leader_ballot_num[0] + 1, self.leader_id)
                            #     # spawn scout to secure adoption
                            #     my_scout = scout.Scout(self.leader_id, self.leader_id, self.leader_ballot_num)
                            #     print "Scout # " + self.leader_id + " started at " + str(my_scout.scout_address)
                            #     my_scout.start()

                            #print "leader backoff for random amount of time"
                            #time.sleep(random.randint(backoff_lower_bound, backoff_upper_bound))
                            self.last_heard_leader_time = time.time()

                    elif msg["type"] == "keepalive":
                        print "heard keepalive"
                        print "am i active? " + str(self.active) 
                        self.active = False
                        print "set self to be inactive"
                        self.last_heard_leader_time = time.time()

                    else:
                        print "wrong message received"
                else:
                    print "null message received"

                # close connection
                minion_conn.close()

            except socket.timeout:
                # check the time since last heard from active leader
                # if current leader is inactive
                self.check_time_since_last_heard_leader() 

    def extracted_proposals_of_highest_ballot_number(self, accepted_proposals):
        # accepted_proposals = [(ballot_num, slot_num, proposal_value)]
        extracted_proposals = []
        # find all slot num present
        all_slot_nums = set([proposal["slot_num"] for proposal in accepted_proposals])
        # for each slot_num
        # find the proposal_value with the highest ballot number
        # add this (slot_num, proposal_value) to extracted_proposals
        for slot_num in all_slot_nums:
            # gather all proposals for the same slot num
            proposals_for_slot_num = [proposal 
                        for proposal in accepted_proposals
                        if proposal["slot_num"] == slot_num]
            # sort those proposals
            max_proposal_for_slot_num = max(proposals_for_slot_num, key=lambda k: k["ballot_num"])
            # remove ballot num from proposal
            max_proposal_for_slot_num.pop("ballot_num")      
            # extracted_proposals = [(slot_num, proposal_value)]
            extracted_proposals += [max_proposal_for_slot_num]
            
        return extracted_proposals

    def update_proposals_with_extracted_proposals(self, extracted_proposals):
        # find all slot_num present
        all_slot_nums = set([proposal["slot_num"] for proposal in extracted_proposals])
        
        # find proposals in self.proposals but not in extracted_proposals
        additional_proposals_from_leader = [proposal 
                                            for proposal in self.proposals 
                                            if proposal["slot_num"] not in all_slot_nums]
        # update self.proposals with all extracted_proposals
        # and what's originally in self.proposals but not in extracted_proposals
        # this guarantees that leaders propose the same proposal value for the same slot_num
        self.proposals = list(extracted_proposals)
        self.proposals.extend(additional_proposals_from_leader)

    # def ping_active_leader_timeout(self, active_leader_id):
    #     active_leader_address = tuple(paxos_config["leaders"][active_leader_id])
    #     while True:
    #         try:
    #             leader_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #             leader_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #             leader_sock.connect(active_leader_address)
    #             leader_sock.close()
    #             time.sleep(3.0)
    #         except socket.error:
    #             print "ping active leader timeout"
    #             return True

    def check_time_since_last_heard_leader(self):
        if self.active:
            return
        # leader is inactive
        # send out scout
        if time.time() - self.last_heard_leader_time > active_leader_timeout_threshold:
            self.send_out_scout()

    def check_time_and_send_keepalive(self):
        if time.time() - self.last_send_keepalive > send_keepalive_period:
            for leader_id in self.other_leader_ids:
                    self.send_keepalive(leader_id)

    def send_keepalive(self, leader_id):
        print "send keepalive to leader #" + str(leader_id)
        keepalive_msg = {"type" : "keepalive",
                    "leader_id" : self.leader_id
                    }
        try:
            # connect to leader
            leader_address = tuple(paxos_config["leaders"][self.leader_id])
            leader_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            leader_conn.connect(leader_address)
            # send msg
            leader_conn.sendall(json.dumps(keepalive_msg))
            leader_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to leader # " + str(leader_id)
        # update send time
        self.last_send_keepalive = time.time()

if __name__ == "__main__":

    leader_id = sys.argv[1]

    leader = Leader(leader_id)

    try:
        print "leader # " + leader_id + " started at " + str(leader.leader_address)
        leader.spawn_scouts_and_commanders()

    except KeyboardInterrupt:
        print "leader interrupted"
        sys.exit(0)

    print "leader done and exiting"



