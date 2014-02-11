#!/usr/bin/env python

import sys
import socket
import json
import SocketServer
import subprocess
import threading
import os

import scout
import commander

LOCK_SUCCESS = 0
LOCK_FAILURE = 1
LOCK_WAIT = 3

UNLOCK_SUCCESS = 0
UNLOCK_FAILURE = 1

backlog = 10
maxbuf = 10240

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

class Leader:

    def __init__(self, leader_id):
        
        # network state
        self.leader_address = tuple(paxos_config["leaders"][leader_id])
        self.leader_id = leader_id

        # Paxos state
        self.active = False
        self.leader_ballot_num = 0
        self.proposals = []

    def spawn_scouts_and_commanders(self):

        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.leader_address)
        s.listen(backlog)

        # spawn scout to execute p1 phase
        my_scout = scout.Scout(self.leader_id, self.leader_id, self.leader_ballot_num)
        print "Scout #" + self.leader_id + " started at " + str(my_scout.scout_address)
        my_scout.send_p1a_recv_p1b()

        # event loop
        while 1:
            # listen for minion p1b response
            minion_conn, minion_address = s.accept()
            data = minion_conn.recv(maxbuf).strip()
            if data:
                msg = json.loads(data)
                if msg["type"] == "propose":
                    slot_num = msg["slot_num"]
                    proposal_value = msg["proposal_value"]
                    print "proposal from replica " + str(msg)
                    if slot_num not in self.proposals:
                        new_proposal = {"slot_num" : slot_num,
                                        "proposal_value" : proposal_value
                                        }
                        self.proposals += [new_proposal]
                    if self.active:
                        # spawn a commander to execute p2 phase
                        proposal = {"ballot_num" : self.leader_ballot_num,
                                    "slot_num" : slot_num,
                                    "proposal_value" : proposal_value}
                        my_commander = commander.Commander(self.leader_id, self.leader_id, proposal)
                        my_commander.send_p2a_recv_p2b()

                elif msg["type"] == "adopted":
                    print "leader recv adopted message"
                    self.active = True
                    print "leader become active"
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
                        print "leader #" + self.leader_id + "spawning commander for proposal " + str(proposal_with_leader_ballot_num)
                        my_commander.send_p2a_recv_p2b()       

                elif msg["type"] == "preempted":
                    if msg["ballot_num"] > self.leader_ballot_num:
                        self.active = False
                        print "leader become inactive"
                        self.leader_ballot_num += 1
                        # spawn scout to secure adoption
                        my_scout = scout.Scout(self.leader_id, self.leader_id, self.leader_ballot_num)
                        print "Scout # " + self.leader_id + " started at " + str(my_scout.scout_address)
                        my_scout.send_p1a_recv_p1b()

                else:
                    print "wrong message received"
            else:
                print "null message received"

            # close connection
            minion_conn.close()

    def extracted_proposals_of_highest_ballot_number(self, accepted_proposals):
        # accepted_proposals = [(b, s, p)]
        extracted_proposals = []
        # find all slot num present
        all_slot_nums = set([proposal["slot_num"] for proposal in accepted_proposals])
        for slot_num in all_slot_nums:
            # gather all proposals for the same slot num
            proposals_for_slot_num = [proposal 
                        for proposal in accepted_proposals
                        if proposal["slot_num"] == slot_num]
            # sort those proposals
            max_proposal_for_slot_num = max(proposals_for_slot_num, key=lambda k: k["ballot_num"])

            # remove ballot num from proposal
            max_proposal_for_slot_num.pop("ballot_num")      
            extracted_proposals += [max_proposal_for_slot_num]
        
        return extracted_proposals

    def update_proposals_with_extracted_proposals(self, extracted_proposals):
        # find all slot num present
        all_slot_nums = set([proposal["slot_num"] for proposal in extracted_proposals])
        
        # find proposals in self.proposals but not in extracted_proposals
        additional_proposals_from_leader = [proposal 
                                            for proposal in self.proposals 
                                            if proposal["slot_num"] not in all_slot_nums]

        self.proposals = list(extracted_proposals)
        self.proposals.extend(additional_proposals_from_leader)


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



