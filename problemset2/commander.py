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

recv_decision_timeout = 5.0

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

def pprint(msg):
    print json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))

class Commander(threading.Thread):

    def __init__(self, leader_id, commander_id, proposal):
        threading.Thread.__init__(self)
        
        # network state
        self.commander_address = tuple(paxos_config["commanders"][commander_id])
        self.leader_id = leader_id

        # Paxos state
        self.commander_id = commander_id
        self.proposal = proposal

    def run(self):
        self.send_p2a_recv_p2b()

    def send_p2a_recv_p2b(self):
        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # this binds to a port assigned by OS
        # there may be multiple scouts for the same leader
        # so cannot assign a fix port for scouts
        s.bind(self.commander_address)
        self.commander_address = s.getsockname()
        s.listen(backlog)
        s.settimeout(recv_decision_timeout)

        # send p2a to all acceptors
        acceptor_ids = paxos_config["acceptors"].keys()
        for acceptor_id in acceptor_ids:
            self.send_p2a(acceptor_id)

        # waiting list of acceptors
        wait_for_acceptor_ids = acceptor_ids

        # event loop
        while 1:
            print "commander # " + self.commander_id + " listening"
            
            try:
                # listen for acceptor p1b response
                acceptor_conn, acceptor_address = s.accept()
                acceptor_conn.settimeout(None)
                data = acceptor_conn.recv(maxbuf).strip()
                if data:
                    msg = json.loads(data)
                    if msg["type"] == "p2b":
                        acceptor_id = msg["acceptor_id"]
                        acceptor_ballot_num = tuple(msg["ballot_num"])
                        print "response from acceptor# " + str(acceptor_id)
                        pprint(msg)
                        # if acceptor adopted leader_ballot_num
                        # remove acceptor from waiting list
                        if acceptor_ballot_num == self.proposal["ballot_num"]:
                            if acceptor_id in wait_for_acceptor_ids:
                                wait_for_acceptor_ids.remove(acceptor_id)
                            # if heard from quorum of acceptors adopting proposal
                            # send "decision" to all replicas
                            if len(wait_for_acceptor_ids) <= len(acceptor_ids) / 2:
                                print "quorum reached"
                                replica_ids = paxos_config["replicas"].keys()
                                for replica_id in replica_ids:
                                    print " SEND decision to replica # " + replica_id
                                    self.send_decision(replica_id)
                                # completes accept phase
                                print "commander # " + self.commander_id + " exiting after decision"
                                s.close()
                                return
                        else:
                            # acceptors already adopted a higher leader_ballot_num
                            # leader needs to be pre-empted
                            preempted_msg = self.generate_preempted(acceptor_ballot_num)
                            self.send_preempted(acceptor_ballot_num)
                            print "commander # " + self.commander_id + " exiting after preemption"
                            s.close()
                            return
                    else:
                        print "wrong message received"
                else:
                    print "null message received"

                # close connection
                acceptor_conn.close()

            except socket.timeout:
                # send p2a to all acceptors
                acceptor_ids = paxos_config["acceptors"].keys()
                for acceptor_id in acceptor_ids:
                    self.send_p2a(acceptor_id)

    def generate_p2a(self):
        p2a_msg = {"type" : "p2a",
                    "leader_id" : self.commander_id,
                    "proposal" : self.proposal,
                    "commander_address" : self.commander_address
                  }
        return p2a_msg

    def generate_decision(self):
        decision_msg = {"type" : "decision",
                        "slot_num" : self.proposal["slot_num"],
                        "proposal_value" : self.proposal["proposal_value"]
                      }
        return decision_msg

    def generate_preempted(self, ballot_num):
        preempted_msg = {"type" : "preempted",
                        "ballot_num" : ballot_num
                        }
        return preempted_msg


    def send_p2a(self, acceptor_id):
        print "SEND p2a to acceptor # " + acceptor_id
        try:
            # create accceptor socket
            acceptor_address = tuple(paxos_config["acceptors"][acceptor_id])
            acceptor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            acceptor_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            acceptor_sock.connect(acceptor_address)
            # send message to acceptor
            p2a_msg = self.generate_p2a()
            acceptor_sock.sendall(json.dumps(p2a_msg))
            acceptor_sock.close()
        except socket.error, (value,message): 
            print "Could not connect to acceptor # " + str(acceptor_id) 

    def send_decision(self, replica_id):
        try:
            # create replica socket
            replica_address = tuple(paxos_config["replicas"][replica_id])
            replica_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            replica_conn.connect(replica_address)
            # send message to acceptor
            decision_msg = self.generate_decision()
            replica_conn.sendall(json.dumps(decision_msg))
            replica_conn.close()
            print " SEND decision to replica # " + str(replica_id)
            pprint(decision_msg)
        except socket.error, (value,message): 
            print "Could not connect to replica # " + str(replica_id)

    def send_preempted(self, acceptor_ballot_num):
        preempted_msg = self.generate_preempted(acceptor_ballot_num)
        print "ready to send to leader preempted_msg message" + str(preempted_msg)
        
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

    def send_to_leader(self, msg):
        print "ready to send to leader adopted message" + str(msg)

        try:
            # connect to leader
            leader_address = tuple(paxos_config["leaders"][self.leader_id])
            leader_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            leader_conn.connect(leader_address)
            # send msg
            leader_conn.sendall(json.dumps(msg))
            leader_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to leader # " + str(self.leader_id)

if __name__ == "__main__":

    commander_id = sys.argv[1]
    leader_id = sys.argv[2]

    proposal = {"ballot_num" : (3, leader_id), "slot_num" : 1, "proposal_value" : "lock 1"}
    commander = Commander(leader_id, commander_id, proposal)

    try:
        print "commander # " + commander_id + " started at " + str(commander.commander_address)
        commander.send_p2a_recv_p2b()

    except KeyboardInterrupt:
        print "commander interrupted"
        sys.exit(0)

    print "commander done and exiting"



