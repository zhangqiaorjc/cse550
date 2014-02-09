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

server_ip = '127.0.0.1'
PORT = 1111


class LockClient:

    def connect_to_server(self, server_ip, port):
        # send create circuit request to ip
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.connect((server_ip, port))

    def generate_request(self, client_id, command_id, op):
        request_msg = {"type" : "request",
                        "command" : {"client_id" : client_id,
                                     "command_id" : command_id,
                                      "op" : op
                                    }}
        return request_msg

    def send_request_recv_response(self, client_id, command_id, op):
        request_msg = self.generate_request(client_id, command_id, op)
        self.sock.send(json.dumps(request_msg))

        maxbuf = 1024
        data = self.sock.recv(maxbuf).strip()
        if data:
            response = json.loads(data)
            result = response["result"]
            print "cid = " + str(result["command_id"])
            print "result_code = " + str(result["result_code"])        
      
if __name__ == "__main__":
    client = LockClient()
    client.connect_to_server(server_ip, PORT)
    client.send_request_recv_response(1, 1, "lock 1")

    client.connect_to_server(server_ip, PORT)
    client.send_request_recv_response(1, 2, "lock 2")

    time.sleep(4)

    client.connect_to_server(server_ip, PORT)
    client.send_request_recv_response(1, 3, "unlock 1")
    
    client.connect_to_server(server_ip, PORT)
    client.send_request_recv_response(1, 4, "unlock 2")

