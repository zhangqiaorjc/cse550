#!/usr/bin/env python

import sys
import socket
import json
import SocketServer
import subprocess
import threading
import os

import time

LOCK_SUCCESS = 0
LOCK_FAILURE = 1
LOCK_WAIT = 3

UNLOCK_SUCCESS = 0
UNLOCK_FAILURE = 1

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

backlog = 10
maxbuf = 10240

class LockClient:

    def __init__(self, client_id):
        self.client_id = client_id
        self.client_address = tuple(paxos_config["lock_clients"][client_id])
        print self.client_address

    def send_request_to_replica(self, replica_id, request_msg):
        # send request to replica
        print "client # " + str(self.client_id) \
                + " send request " + str(request_msg) \
                + "to replica # " + str(replica_id)
        replica_address = tuple(paxos_config["replicas"][replica_id])
        replica_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replica_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        replica_conn.connect(replica_address)
        replica_conn.sendall(json.dumps(request_msg))
        replica_conn.close()

    def generate_request(self, client_id, command_id, op):
        request_msg = {"type" : "request",
                        "command" : {"client_id" : client_id,
                                     "command_id" : command_id,
                                      "op" : op
                                    }}
        return request_msg

    def send_request_recv_response(self, command_id, op):
        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.client_address) 
        s.listen(backlog)

        # generate request
        request_msg = self.generate_request(self.client_id, command_id, op)
        # send request to each server
        for replica_id in paxos_config["replicas"].keys():
            self.send_request_to_replica(replica_id, request_msg)
        
        # recv response
        replica_conn, address = s.accept()
        data = replica_conn.recv(maxbuf).strip()
        if data:
            msg = json.loads(data)
            if msg["type"] == "response":
                print "client # " + self.client_id + " recv response from replica " 
                result = msg["result"]
                print "command_id = " + str(result["command_id"])
                print "result_code = " + str(result["result_code"])
        replica_conn.close()
        s.close()

if __name__ == "__main__":
    client_id = sys.argv[1]
    client = LockClient(client_id)

    print "Lock client # " + client_id + " started at " + str(client.client_address)
        

    command_lists = []
    command_lists += [("0", "lock 0")]
    #command_lists += [("1", "lock 1")]
    #command_lists += [("2", "unlock 0")]
    #command_lists += [("3", "unlock 1")]

    for command in command_lists:
        if command[0] == "2":
            time.sleep(4)
        client.send_request_recv_response(command[0], command[1])

    print "client done and exiting"

