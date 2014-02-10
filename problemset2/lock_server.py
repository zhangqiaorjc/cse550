import sys
import socket 
import Queue
import json

AVAILABLE = 0
UNAVAILABLE = 1

NO_OWNER = 0

SUCCESS = 0
FAILURE = 1

LOCK_SUCCESS = 0
LOCK_FAILURE = 1
LOCK_WAIT = 3

UNLOCK_SUCCESS = 0
UNLOCK_FAILURE = 1

MAX_PAXOS_INSTANCES = 1000

NUM_LOCKS = 20

backlog = 10
maxbuf = 1024

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())


class LockServer:
    def __init__(self, lock_server_id, num_locks):
        # server states
        self.lock_server_id = lock_server_id
        self.lock_server_address = tuple(paxos_config["replicas"][str(lock_server_id)])

        # map from client_id to client_conn
        self.client_conns = {}
        self.client_commands = {}

        # lock states
        self.num_locks = num_locks
        self.lock_states = []
        self.lock_owners = []
        self.lock_wait_queues = []
        for i in xrange(num_locks):
            self.lock_states += [AVAILABLE]
            self.lock_owners += [NO_OWNER]
            self.lock_wait_queues += [Queue.Queue()]

        # Paxos state
        self.decisions = {}
        self.proposals = {}
        self.slot_num = 0

    def generate_propose(self):
        propose_msg = {"type" : "propose",
                        "slot_num" : self.slot_num,
                        "proposal_value" : self.proposal_value
                        }
        return propose_msg

    def send_propose(self, leader_id):
        # create accceptor socket
        leader_address = tuple(paxos_config["leaders"][leader_id])
        leader_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        leader_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        leader_sock.connect(leader_address)

        # send message to leader
        propose_msg = self.generate_propose()
        leader_sock.send(json.dumps(propose_msg))
        leader_sock.close()

    def lock(self, x, client_id):   
        print "trying to lock " + str(x) + "from " + str(client_id)

        # error cases
        if (x < 0 or x >= self.num_locks):
            # lock doesn't exist
            self.reply_to_client(client_id, LOCK_FAILURE)
            return LOCK_FAILURE
        
        if (self.lock_owners[x] == client_id):
            # repeated lock by the same client
            self.reply_to_client(client_id, LOCK_SUCCESS)
            return LOCK_SUCCESS

        # acquire lock
        if (self.lock_states[x] == AVAILABLE
            and self.lock_owners[x] == NO_OWNER):
            # lock
            self.lock_states[x] = UNAVAILABLE
            self.lock_owners[x] = client_id
            self.reply_to_client(client_id, LOCK_SUCCESS)
            return LOCK_SUCCESS
        else:
            # lock held by someone else
            # client blocked
            self.lock_wait_queues[x].put(client_id)
            return LOCK_WAIT

    def unlock(self, x, client_id):
        print "trying to unlock " + str(x) + "from " + str(client_id)

        # error cases
        if (x < 0 or x >= self.num_locks):
            # lock doesn't exist
            self.reply_to_client(client_id, UNLOCK_FAILURE)
            return UNLOCK_FAILURE

        if (self.lock_owners[x] != client_id):
            # unlock someone else's lock
            self.reply_to_client(client_id, UNLOCK_FAILURE)
            return UNLOCK_FAILURE

        if (self.lock_owners[x] == NO_OWNER):
            # unlock an available lock
            self.reply_to_client(client_id, UNLOCK_SUCCESS)
            return UNLOCK_SUCCESS


        if (self.lock_owners[x] == client_id
            and self.lock_states[x] == UNAVAILABLE):
            if not self.lock_wait_queues[x].empty():
                # someone else waiting for the lock
                # hand over lock
                waiting_client_id = self.lock_wait_queues[x].get()
                self.lock_owners[x] = waiting_client_id
                self.reply_to_client(waiting_client_id, LOCK_SUCCESS)
            else:
                # no one waiting for the lock
                self.lock_owners[x] = NO_OWNER
                self.lock_states[x] = AVAILABLE

            self.reply_to_client(client_id, UNLOCK_SUCCESS)
            return UNLOCK_SUCCESS

    def generate_response(self, command_id, result):
        response_msg = {"type" : "response",
                        "result" : {"command_id" : command_id,
                                      "result_code" : result
                                    }}
        return response_msg


    def reply_to_client(self, client_id, response_state):
        client_conn = self.client_conns[client_id]
        command_id = self.client_commands[client_id]["command_id"]
        response_msg = self.generate_response(command_id, response_state)
        client_conn.send(json.dumps(response_msg))
        client_conn.close()

    def perform(self, command):
        # TODO
        self.decisions += [command]

        client_id = command["client_id"]
        op = command["op"].split(" ")
        opcode = op[0]
        lock_num = int(op[1])
        if opcode == "lock":
            self.lock(lock_num, client_id)
        elif opcode == "unlock":
            self.unlock(lock_num, client_id)
        else:
            print "error"


    def serve_forever(self):

        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.lock_server_address) 
        s.listen(backlog)

        # event loop
        while 1:
            client_conn, address = s.accept() 
            data = client_conn.recv(maxbuf).strip()
            if data: 
                msg = json.loads(data)
                if msg["type"] == "request":
                        command = msg["command"]
                        client_id = command["client_id"] 
                        self.client_conns[client_id] = client_conn
                        self.client_commands[client_id] = command
                        self.perform(command)
                else:
                    print "wrong message received"
                    client_conn.close()
            else:
                print "null message received"
                client_conn.close()


if __name__ == "__main__":
    lock_server_id = sys.argv[1]

    server = LockServer(lock_server_id, NUM_LOCKS)
    try:
        print "Lock Server #" + lock_server_id + " started at " + str(server.lock_server_address)
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)