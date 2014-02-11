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

backlog = 10
maxbuf = 10240

paxos_config_file = open("paxos_group_config.json", "r")
paxos_config = json.loads(paxos_config_file.read())

class Acceptor:
    def __init__(self, acceptor_id):
        # server states
        self.acceptor_address = tuple(paxos_config["acceptors"][acceptor_id])
        
        # Paxos state
        self.ballot_num = -1
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


    def reply_to_scout(self, leader_id, msg):
        # use scout port
        scout_address = tuple(paxos_config["scouts"][leader_id])
        print "reply to scout #" + str(leader_id) + " msg = " + str(msg)
        
        # create leader connection
        scout_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        scout_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            scout_conn.connect(scout_address)
        except socket.error, (value,message): 
            if scout_conn: 
                scout_conn.close() 
            print "Could not open socket: " + message 
            sys.exit(0)   

        # send message
        scout_conn.send(json.dumps(msg))
        scout_conn.close()

    def reply_to_commander(self, leader_id, msg):
        # use commander port
        commander_address = tuple(paxos_config["commanders"][leader_id])
        print "reply to commander #" + str(leader_id) + " msg = " + str(msg)
        
        # create leader connection
        commander_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        commander_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        commander_conn.connect(commander_address)

        # send message
        commander_conn.send(json.dumps(msg))
        commander_conn.close()

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
                    print "recv p1a"
                    leader_id = msg["leader_id"]
                    # asked to prepare ballot number b
                    leader_ballot_num = msg["ballot_num"] 
                    
                    if leader_ballot_num > self.ballot_num:
                        # if prepare msg has a larger ballot num
                        # promise to a larger ballot num
                        print "promise to a larger ballot num = " \
                            + str(leader_ballot_num) 
                        self.ballot_num = leader_ballot_num

                    p1b_msg = self.generate_p1b()
                    self.reply_to_scout(leader_id, p1b_msg)

                elif msg["type"] == "p2a":
                    print "recv p2a"
                    leader_id = msg["leader_id"]
                    proposal = msg["proposal"]
                    leader_ballot_num = proposal["ballot_num"] 

                    if leader_ballot_num >= self.ballot_num:
                        print "accept proposal with ballot_num = "\
                            + str(leader_ballot_num) 
                        self.ballot_num = leader_ballot_num
                        self.accepted_proposals += [proposal]

                    p2b_msg = self.generate_p2b()
                    self.reply_to_commander(leader_id, p2b_msg)
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
        print "Acceptor #" + acceptor_id + " started at " + str(server.acceptor_address)
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)