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

backlog = 5
maxbuf = 10240

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

def pprint(msg):
    print json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))

class LockServer:
    def __init__(self, replica_id, num_locks):
        # server states
        self.replica_id = replica_id
        self.replica_address = tuple(paxos_config["replicas"][str(replica_id)])

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
        self.decisions = []
        self.proposals = [] # (s, p)
        self.slot_num = 0
    
    def lock(self, client_id, command_id, x):
        print "trying to lock " + str(x) + " for client# " + str(client_id)

        # error cases
        if (x < 0 or x >= self.num_locks):
            # lock doesn't exist
            self.reply_to_client(client_id, command_id, LOCK_FAILURE)
            return LOCK_FAILURE
        
        if (self.lock_owners[x] == client_id):
            # repeated lock by the same client
            self.reply_to_client(client_id, command_id, LOCK_SUCCESS)
            return LOCK_SUCCESS

        # acquire lock
        if (self.lock_states[x] == AVAILABLE
            and self.lock_owners[x] == NO_OWNER):
            # lock
            self.lock_states[x] = UNAVAILABLE
            self.lock_owners[x] = client_id
            self.reply_to_client(client_id, command_id, LOCK_SUCCESS)
            return LOCK_SUCCESS
        else:
            # lock held by someone else
            # client blocked
            self.lock_wait_queues[x].put((client_id, command_id))
            return LOCK_WAIT

    def unlock(self, client_id, command_id, x):
        print "trying to unlock " + str(x) + " for client# " + str(client_id)

        # error cases
        if (x < 0 or x >= self.num_locks):
            # lock doesn't exist
            self.reply_to_client(client_id, command_id, UNLOCK_FAILURE)
            return UNLOCK_FAILURE

        if (self.lock_owners[x] != client_id):
            # unlock someone else's lock
            self.reply_to_client(client_id, command_id, UNLOCK_FAILURE)
            return UNLOCK_FAILURE

        if (self.lock_owners[x] == NO_OWNER):
            # unlock an available lock
            self.reply_to_client(client_id, command_id, UNLOCK_SUCCESS)
            return UNLOCK_SUCCESS

        if (self.lock_owners[x] == client_id
            and self.lock_states[x] == UNAVAILABLE):
            if not self.lock_wait_queues[x].empty():
                # someone else waiting for the lock
                # hand over lock
                (waiting_client_id, waiting_command_id) = self.lock_wait_queues[x].get()
                print "hand over lock from " + str(self.lock_owners[x]) + " to " + str(waiting_client_id)
                self.lock_owners[x] = waiting_client_id
                self.reply_to_client(waiting_client_id, waiting_command_id, LOCK_SUCCESS)
            else:
                # no one waiting for the lock
                self.lock_owners[x] = NO_OWNER
                self.lock_states[x] = AVAILABLE

            self.reply_to_client(client_id, command_id, UNLOCK_SUCCESS)
            return UNLOCK_SUCCESS

    def serve_forever(self):
        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.replica_address)
        s.listen(backlog)
        s.settimeout(5.0)

        # event loop
        while 1:
            print "listening"
            try:
                client_conn, address = s.accept()
                client_conn.settimeout(None)
                # if server does receive a message
                data = client_conn.recv(maxbuf).strip()
                if data: 
                    msg = json.loads(data)
                    if msg["type"] == "request":
                        print "request msg recv"
                        pprint(msg)
                        proposal_value = msg["command"]
                        # propose to all leaders
                        self.propose(proposal_value)

                    elif msg["type"] == "decision":
                        print "decision msg recv"
                        pprint(msg)
                        new_decision = {"slot_num" : msg["slot_num"],
                                        "proposal_value" : msg["proposal_value"]
                                        }
                        if new_decision not in self.decisions:
                            self.decisions += [new_decision]
                        
                        # find decided proposal_value for self.slot_num
                        # propose the distinct proposal values that had self.slot_num with a different slot_num
                        decision_proposal_values_for_slot_num = [decision["proposal_value"]
                                                                    for decision in self.decisions
                                                                    if decision["slot_num"] == self.slot_num]
                        conflicted_proposal_values = [proposal["proposal_value"] 
                                                        for proposal in self.proposals
                                                        if (proposal["slot_num"] == self.slot_num)
                                                        and (proposal["proposal_value"] not in decision_proposal_values_for_slot_num)]
                        print "conflicted_proposal_values"
                        print conflicted_proposal_values
                        # propose conflicted proposal values
                        for proposal_value in conflicted_proposal_values:
                            self.propose(proposal_value)
                        # perform the decided proposal values for self.slot_num
                        # skip performining proposal values that have higher slot_num
                        for proposal_value in decision_proposal_values_for_slot_num:
                            self.perform(proposal_value)

                    elif msg["type"] == "query":
                        print "query msg recv"
                        pprint(msg)
                        query_replica_id = msg["replica_id"]
                        query_slot_num = msg["slot_num"]
                        for decision in self.decisions:
                            if decision["slot_num"] == query_slot_num:
                                self.send_query_answer(query_replica_id, decision)
                    else:
                        print "wrong message received"
                        client_conn.close()
                else:
                    print "null message received"
                    client_conn.close()

            except socket.timeout:
                # the server could get stuck waiting even though
                # there might be outstanding decision messages
                # it has not received
                # so proactively probe other replicas for decision message
                print "replica timeout on listening"
                replica_ids = paxos_config["replicas"].keys()
                replica_ids.remove(self.replica_id)
                for replica_id in replica_ids:
                    print "send query on replica to replica # " +\
                        replica_id + " for slot_num " + str(self.slot_num)
                    self.send_query(replica_id, self.slot_num)

    def propose(self, proposal_value):
        # only add to self.proposals if not already exist
        if len(self.proposals) > 0:
            all_existing_proposal_values = [proposal["proposal_value"]
                                    for proposal in self.proposals]

            proposal_value_already_exist = [True for pval in all_existing_proposal_values 
                                                if pval["client_id"] == proposal_value["client_id"]
                                                and pval["command_id"] == proposal_value["command_id"]
                                                and pval["op"] == proposal_value["op"]
                                            ]
            if any(proposal_value_already_exist):
                # proposal_value already in self.proposals
                return

        # find smallest unused slot_num
        min_slot_num = self.find_smallest_unused_slot_num()
        new_proposal = {"slot_num" : min_slot_num,
                        "proposal_value" : proposal_value
                        }
        if new_proposal not in self.proposals:
            self.proposals += [new_proposal]

        # propose to all leaders
        leader_ids = paxos_config["leaders"].keys()
        for leader_id in leader_ids:
            print "propose to leader # " + leader_id + " " + str(new_proposal)
            self.send_propose(leader_id, min_slot_num, proposal_value)

    def find_smallest_unused_slot_num(self):
        all_proposal_slot_nums = set([proposal["slot_num"]
                                    for proposal in self.proposals])
        all_decision_slot_nums = set([decision["slot_num"]
                                    for decision in self.decisions])
        all_used_slot_num = all_proposal_slot_nums.union(all_decision_slot_nums)
        min_slot_num = 0
        while (min_slot_num in all_used_slot_num):
            min_slot_num += 1

        return min_slot_num

    def generate_propose(self, slot_num, proposal_value):
        propose_msg = {"type" : "propose",
                        "slot_num" : slot_num,
                        "proposal_value" : proposal_value
                        }
        return propose_msg

    def send_propose(self, leader_id, slot_num, proposal_value):
        print "ready to send propose to leader # " + leader_id
        try:
            # create leader socket
            leader_address = tuple(paxos_config["leaders"][leader_id])
            leader_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            leader_sock.connect(leader_address)

            # send message to leader
            propose_msg = self.generate_propose(slot_num, proposal_value)
            leader_sock.sendall(json.dumps(propose_msg))
            leader_sock.close()
        except socket.error, (value,message): 
            print "Could not connect to leader # " + str(leader_id)


    def generate_response(self, client_id, command_id, result_code):
        response_msg = {"type" : "response",
                        "client_id" : client_id,
                        "result" : {"command_id" : command_id,
                                      "result_code" : result_code
                                    }}
        return response_msg


    def reply_to_client(self, client_id, command_id, result_code):
        print "ready to reply to client # " + client_id
        try:
            # connect to client
            client_address = tuple(paxos_config["lock_clients"][client_id])
            client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_conn.connect(client_address)

            response_msg = self.generate_response(client_id, command_id, result_code)

            client_conn.sendall(json.dumps(response_msg))
            client_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to client # " + str(client_id)

    def generate_query(self, slot_num):
        query_msg = {"type" : "query",
                        "replica_id" : self.replica_id,
                        "slot_num" : slot_num
                        }
        return query_msg

    def send_query(self, replica_id, slot_num):
        print "ready to send query to replica # " + replica_id
        try:
            # create leader socket
            replica_address = tuple(paxos_config["replicas"][replica_id])
            replica_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            replica_conn.connect(replica_address)

            # send message to leader
            query_msg = self.generate_query(slot_num)
            replica_conn.sendall(json.dumps(query_msg))
            replica_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to leader # " + str(replica_id)    


    def generate_query_answer(self, decision):
        decision_msg = {"type" : "decision",
                        "slot_num" : decision["slot_num"],
                        "proposal_value" : decision["proposal_value"]
                      }
        return decision_msg

    def send_query_answer(self, replica_id, decision):
        try:
            # create replica socket
            replica_address = tuple(paxos_config["replicas"][replica_id])
            replica_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            replica_conn.connect(replica_address)
            # send message to replica
            decision_msg = self.generate_query_answer(decision)
            replica_conn.sendall(json.dumps(decision_msg))
            replica_conn.close()
            print " SEND decision to replica # " + str(replica_id)
            pprint(decision_msg)
        except socket.error, (value,message): 
            print "Could not connect to replica # " + str(replica_id)


    def perform(self, proposal_value):
        # if decision already performed
        # increment slot_num
        # not perform again
        for decision in self.decisions:
            if decision["proposal_value"] == proposal_value \
                and decision["slot_num"] < self.slot_num:
                self.slot_num += 1
                print "ever here?"
                print "self.slot_num: = " + str(self.slot_num)
                return
        
        # if new proposal_value
        # perform it
        print "perform slot_num = " + str(self.slot_num) \
            + " proposal = " + str(proposal_value)
        self.slot_num += 1
        client_id = proposal_value["client_id"]
        command_id = proposal_value["command_id"]
        op = proposal_value["op"].split(" ")
        opcode = op[0]
        lock_num = int(op[1])
        if opcode == "lock":
            self.lock(client_id, command_id, lock_num)
        elif opcode == "unlock":
            self.unlock(client_id, command_id, lock_num)
        else:
            print "wrong op_code request by client"

if __name__ == "__main__":
    replica_id = sys.argv[1]

    server = LockServer(replica_id, NUM_LOCKS)
    try:
        print "Lock Server # " + replica_id + " started at " + str(server.replica_address)
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)