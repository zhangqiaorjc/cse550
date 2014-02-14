#!/usr/bin/env python

import sys
import socket
import json
import SocketServer
import subprocess
import threading
import os

import collections

import time

LOCK_SUCCESS = 0
LOCK_FAILURE = 1
LOCK_WAIT = 3

UNLOCK_SUCCESS = 0
UNLOCK_FAILURE = 1

SUCCESS = 0
FAILURE = 1

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

backlog = 5
maxbuf = 10240

num_connection_attempts = 3
recv_response_timeout = 5.0


def pprint(msg):
    print json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))

class LockClient:

    def __init__(self, client_id):
        self.client_id = client_id
        self.client_address = tuple(paxos_config["lock_clients"][client_id])
        self.replica_ids = paxos_config["replicas"].keys()

    def send_request_to_replica(self, replica_id, request_msg):
        # send request to replica
        #print "client # " + str(self.client_id) \
        #        + " send request " + str(request_msg) \
        #        + "to replica # " + str(replica_id)

        attempt = 0
        while attempt < num_connection_attempts:
            try:
                replica_address = tuple(paxos_config["replicas"][replica_id])
                replica_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                replica_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                replica_conn.connect(replica_address)
                replica_conn.sendall(json.dumps(request_msg))
                replica_conn.close()
                return SUCCESS
            except socket.error, (value,message): 
                print "Could not connect to replica # " + str(replica_id)
                print "try again"
                attempt += 1
        print "Could not connect to replica # " + str(replica_id) \
            + " after " + str(num_connection_attempts) + " attempts"
        return FAILURE


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
        available_replica_ids = self.replica_ids
        for replica_id in available_replica_ids:
            if self.send_request_to_replica(replica_id, request_msg) == FAILURE:
                # delete replica_id from replica list
                self.replica_ids.remove(replica_id)
                if len(self.replica_ids) == 0:
                    print "no more replica/proposers are reachable"
                    sys.exit(1)

    def service_commands_queues(self, command_queue):
        # create listening socket for response
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.client_address) 
        s.settimeout(recv_response_timeout)
        s.listen(backlog)
        
        # return only if client hears the response
        # for the same command_id of its request
        while len(command_queue) > 0:
            # send request
            command = command_queue.popleft()
            # delay sending request
            if command[0] == "delay":
                print "delay for " + command[1] + " sec before sending next request"
                time.sleep(int(command[1]))
                continue
            self.send_request_recv_response(command[0], command[1])

            # recv response
            while True:
                try:
                    replica_conn, address = s.accept()
                    replica_conn.settimeout(None)
                    data = replica_conn.recv(maxbuf).strip()
                    if data:
                        msg = json.loads(data)
                        if msg["type"] == "response":
                            print "client # " + self.client_id + " recv response from replica "
                            # the response is not from an earlier request
                            if msg["command_id"] == command[0]:
                                print "response from replica is "
                                pprint(msg)
                                replica_conn.close()
                                break
                            else:
                                print "response for an earlier request, so discard"
                    replica_conn.close()
                except socket.timeout:
                    # resend request until response comes back
                    print "recv response from replica times out; resend request"
                    self.send_request_recv_response(command[0], command[1])
        # close listening socket once all queue of requests got their responses
        s.close()


if __name__ == "__main__":
    client_id = sys.argv[1]
    client = LockClient(client_id)

    print "Lock client # " + client_id + " started at " + str(client.client_address)
    
    command_queue = collections.deque()
    command_queue.append(("0", "lock 0"))
    command_queue.append(("1", "lock 1"))
    command_queue.append(("delay", "5"))
    command_queue.append(("2", "unlock 0"))
    command_queue.append(("3", "unlock 1"))

    client.service_commands_queues(command_queue)

    print "client done and exiting"

