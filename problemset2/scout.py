#!/usr/bin/env python

import sys
import socket
import json
import SocketServer
import subprocess
import threading
import os

LOCK_SUCCESS = 0
LOCK_FAILURE = 1
LOCK_WAIT = 3

UNLOCK_SUCCESS = 0
UNLOCK_FAILURE = 1

backlog = 5
maxbuf = 10240

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

class Scout:

    def __init__(self, leader_id, scout_id, leader_ballot_num):
        
        # network state
        self.scout_address = tuple(paxos_config["scouts"][scout_id])
        self.leader_id = leader_id

        # Paxos state
        self.scout_id = scout_id
        self.leader_ballot_num = leader_ballot_num
        self.accepted_proposals = []

    def generate_p1a(self):
        p1a_msg = {"type" : "p1a",
                    "leader_id" : self.scout_id,
                    "ballot_num" : self.leader_ballot_num
                  }
        return p1a_msg

    def generate_adopted(self):
        adopted_msg = {"type" : "adopted",
                        "ballot_num" : self.leader_ballot_num,
                        "accepted_proposals" : self.accepted_proposals
                      }
        return adopted_msg

    def generate_preempted(self, ballot_num):
        preempted_msg = {"type" : "preempted",
                        "ballot_num" : ballot_num
                        }
        return preempted_msg

    def send_p1a(self, acceptor_id):
        try:
            # create accceptor socket
            acceptor_address = tuple(paxos_config["acceptors"][acceptor_id])
            acceptor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            acceptor_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            acceptor_sock.connect(acceptor_address)
            # send message to acceptor
            p1a_msg = self.generate_p1a()
            print "SEND p1a to acceptor_id = " + acceptor_id \
                + " msg = " + str(p1a_msg)
            acceptor_sock.sendall(json.dumps(p1a_msg))
            acceptor_sock.close()
        except socket.error, (value,message): 
            print "Could not connect to acceptor # " + str(acceptor_id)


    def send_p1a_recv_p1b(self):

        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.scout_address)
        s.listen(backlog)

        # send p1a to all acceptors
        acceptor_ids = paxos_config["acceptors"].keys()
        for acceptor_id in acceptor_ids:
            self.send_p1a(acceptor_id)

        wait_for_acceptor_ids = acceptor_ids
        accepted_proposals = [] 

        # event loop
        while 1:
            # listen for acceptor p1b response
            acceptor_conn, acceptor_address = s.accept()
            data = acceptor_conn.recv(maxbuf).strip()
            if data:
                msg = json.loads(data)
                if msg["type"] == "p1b":
                    acceptor_id = msg["acceptor_id"]
                    acceptor_ballot_num = tuple(msg["ballot_num"])
                    accepted_proposals = msg["accepted_proposals"]
                    print "RECV p1b from acceptor # " + str(acceptor_id) \
                        + "msg = "+ str(msg)
                    # if acceptor adopts leader_ballot_num
                    # remove acceptor from waiting list
                    # leader collects accepted_proposals from acceptor
                    if acceptor_ballot_num == self.leader_ballot_num:
                        self.accepted_proposals.extend(accepted_proposals)
                        wait_for_acceptor_ids.remove(acceptor_id)
                        # if heard from quorum of acceptors
                        # tell leader that its ballot num is adopted 
                        if len(wait_for_acceptor_ids) <= len(acceptor_ids) / 2:
                            print "quorum reached"
                            self.send_adopted()
                            # completes a prepare phase
                            return
                    else:
                        # acceptors already adopted a higher leader_ballot_num
                        # prepare phase fails
                        # tell leader that it is preempted
                        self.send_preempted(acceptor_ballot_num)
                        return
                else:
                    print "wrong message received"
            else:
                print "null message received"

            # close connection
            acceptor_conn.close()

    def send_adopted(self):
        adopted_msg = self.generate_adopted()
        print "ready to send to leader adopted message " + str(adopted_msg)

        try:
            # connect to leader
            leader_address = tuple(paxos_config["leaders"][self.leader_id])
            leader_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            leader_conn.connect(leader_address)
            # send msg
            leader_conn.sendall(json.dumps(adopted_msg))
            leader_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to leader # " + str(self.leader_id)

    def send_preempted(self, acceptor_ballot_num):
        preempted_msg = self.generate_preempted(acceptor_ballot_num)
        print "ready to send to leader preempted_msg message " + str(preempted_msg)
    
        try:
            # connect to leader
            leader_address = tuple(paxos_config["leaders"][self.leader_id])
            leader_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            leader_conn.connect(leader_address)
            # send msg
            leader_conn.sendall(json.dumps(preempted_msg))
            leader_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to leader # " + str(self.leader_id)

if __name__ == "__main__":

    scout_id = sys.argv[1]
    leader_id = sys.argv[2]
    leader_ballot_num = int(sys.argv[3])

    scout = Scout(leader_id, scout_id, leader_ballot_num)

    try:
        print "Scout # " + scout_id + " started at " + str(scout.scout_address)
        scout.send_p1a_recv_p1b()

    except KeyboardInterrupt:
        print "scout interrupted"
        sys.exit(0)

    print "scout done and exiting"



