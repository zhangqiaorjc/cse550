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

backlog = 5
maxbuf = 10240

def pprint(msg):
    print json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))

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
        try:
            replica_address = tuple(paxos_config["replicas"][replica_id])
            replica_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            replica_conn.connect(replica_address)
            replica_conn.sendall(json.dumps(request_msg))
            replica_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to replica # " + str(replica_id)

    def generate_request(self, command_id, op):
        request_msg = {"type" : "request",
                        "command" : {"client_id" : self.client_id,
                                     "command_id" : command_id,
                                      "op" : op
                                    }}
        return request_msg

    def send_request_recv_response(self, command_id, op):

        # generate request
        request_msg = self.generate_request(command_id, op)
        # send request to each server
        for replica_id in paxos_config["replicas"].keys():
            self.send_request_to_replica(replica_id, request_msg)

    def service_commands_queues(self, command_queue):
        # create listening socket for response
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.client_address) 
        s.listen(backlog)

        for command in command_queue:
            if command[0] == "2":
                #time.sleep(5)
            self.send_request_recv_response(command[0], command[1])

        # return only if client hears the response
        # for the same command_id of its request
        while 1:
            # recv response
            replica_conn, address = s.accept()
            data = replica_conn.recv(maxbuf).strip()
            if data:
                msg = json.loads(data)
                if msg["type"] == "response":
                    print "client # " + self.client_id + " recv response from replica "
                    pprint(msg)
                    #if msg["result"]["command_id"] == command_id:
                    #break
            replica_conn.close()
        
        s.close()


if __name__ == "__main__":
    client_id = sys.argv[1]
    client = LockClient(client_id)

    print "Lock client # " + client_id + " started at " + str(client.client_address)
    
    command_queue = []
    command_queue += [("0", "lock 0")]
    command_queue += [("1", "lock 1")]
    command_queue += [("2", "unlock 0")]
    command_queue += [("3", "unlock 1")]

    client.service_commands_queues(command_queue)

    print "client done and exiting"

