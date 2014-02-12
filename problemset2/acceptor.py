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

MAX_INSTANCES = 1000

backlog = 5
maxbuf = 10240

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())


def pprint(msg):
    print json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))

class Acceptor:
    def __init__(self, acceptor_id):
        # server states
        self.acceptor_address = tuple(paxos_config["acceptors"][acceptor_id])
        
        # Paxos state
        # tuples are compared in order of fields
        self.ballot_num = (-1, acceptor_id)
        self.acceptor_id = acceptor_id
        self.accepted_proposals = []

    def generate_p1b(self):
        p1b_msg = {"type" : "p1b",
                    "acceptor_id" : self.acceptor_id,
                    "ballot_num" : self.ballot_num,
                    "accepted_proposals" : self.accepted_proposals
                  }
        return p1b_msg

    def generate_p2b(self):
        p1b_msg = {"type" : "p2b",
                    "acceptor_id" : self.acceptor_id,
                    "ballot_num" : self.ballot_num
                  }
        return p1b_msg


    def reply_to_scout(self, leader_id, scout_address, msg):
        print "reply to scout # " + str(leader_id)
        pprint(msg)
        
        try:
            # create leader connection
            scout_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            scout_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            scout_conn.connect(scout_address)
            # send message
            scout_conn.send(json.dumps(msg))
            scout_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to scout # " + str(leader_id) 



    def reply_to_commander(self, leader_id, commander_address, msg):
        print "reply to commander # " + str(leader_id)
        pprint(msg)
        
        # create leader connection
        try:
            commander_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            commander_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            commander_conn.connect(commander_address)
            # send message
            commander_conn.send(json.dumps(msg))
            commander_conn.close()
        except socket.error, (value,message): 
            print "Could not connect to commander # " + str(leader_id)

    def serve_forever(self):

        # create listening socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(self.acceptor_address) 
        s.listen(backlog)

        # event loop
        while 1:
            leader_conn, leader_address = s.accept() 
            data = leader_conn.recv(maxbuf).strip()
            if data: 
                msg = json.loads(data)
                if msg["type"] == "p1a":
                    leader_id = msg["leader_id"]
                    # asked to prepare ballot number b
                    leader_ballot_num = tuple(msg["ballot_num"]) 
                    print "RECV p1a from leader # " + leader_id
                    pprint(msg)
                    if leader_ballot_num > self.ballot_num:
                        # if prepare msg has a larger ballot num
                        # promise to a larger ballot num
                        print "promise to a larger ballot num = " \
                            + str(leader_ballot_num) 
                        self.ballot_num = leader_ballot_num

                    p1b_msg = self.generate_p1b()
                    scout_address = tuple(msg["scout_address"])
                    self.reply_to_scout(leader_id, scout_address, p1b_msg)

                elif msg["type"] == "p2a":
                    leader_id = msg["leader_id"]
                    proposal = msg["proposal"]
                    leader_ballot_num = tuple(proposal["ballot_num"]) 
                    print "RECV p2a from leader # " + leader_id
                    pprint(msg)

                    if leader_ballot_num >= self.ballot_num:
                        print "accept proposal with ballot_num = "\
                            + str(leader_ballot_num)
                        self.ballot_num = leader_ballot_num
                        self.accepted_proposals += [proposal]

                    p2b_msg = self.generate_p2b()
                    commander_address = tuple(msg["commander_address"])
                    self.reply_to_commander(leader_id, commander_address, p2b_msg)
                else:
                    print "wrong message received"
            else:
                print "null message received"
            
            # close connection
            leader_conn.close()


if __name__ == "__main__":
    acceptor_id = sys.argv[1]
    
    server = Acceptor(acceptor_id)
 
    try:
        print "Acceptor # " + acceptor_id + " started at " + str(server.acceptor_address)
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)