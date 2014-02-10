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

class LockClient:

    def __init__(self, client_id):
        self.client_id = client_id

    def connect_to_server(self, lock_server_id):
        lock_server_address = tuple(paxos_config["replicas"][lock_server_id])
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.connect(lock_server_address)

    def generate_request(self, client_id, command_id, op):
        request_msg = {"type" : "request",
                        "command" : {"client_id" : client_id,
                                     "command_id" : command_id,
                                      "op" : op
                                    }}
        return request_msg

    def send_request_recv_response(self, command_id, op):
        request_msg = self.generate_request(
                        self.client_id, command_id, op)
        self.sock.send(json.dumps(request_msg))

        maxbuf = 1024
        data = self.sock.recv(maxbuf).strip()
        if data:
            response = json.loads(data)
            result = response["result"]
            print "cid = " + str(result["command_id"])
            print "result_code = " + str(result["result_code"])        
      
if __name__ == "__main__":
    client_id = int(sys.argv[1])
    client = LockClient(client_id)

    lock_server_id = sys.argv[2]

    client.connect_to_server(lock_server_id)
    client.send_request_recv_response(1, "lock 1")

    client.connect_to_server(lock_server_id)
    client.send_request_recv_response(2, "lock 2")

    time.sleep(4)

    client.connect_to_server(lock_server_id)
    client.send_request_recv_response(3, "unlock 1")
    
    client.connect_to_server(lock_server_id)
    client.send_request_recv_response(4, "unlock 2")

