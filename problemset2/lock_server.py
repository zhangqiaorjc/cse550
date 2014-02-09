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

class LockServer:
	def __init__(self, server_ip, port, num_locks):
		# server states
		self.server_ip = server_ip
		self.port = port
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
		self.decisions = []
		self.proposals = []

	def lock(self, x, client_id):	
		print "trying to lock " + str(x) + "from " + str(client_id)

		# error cases
		if (x < 0 or x >= self.num_locks):
			# lock doesn't exist
			self.reply(client_id, LOCK_FAILURE)
			return LOCK_FAILURE
		
		if (self.lock_owners[x] == client_id):
			# repeated lock by the same client
			self.reply(client_id, LOCK_SUCCESS)
			return LOCK_SUCCESS

		# acquire lock
		if (self.lock_states[x] == AVAILABLE
			and self.lock_owners[x] == NO_OWNER):
			# lock
			self.lock_states[x] = UNAVAILABLE
			self.lock_owners[x] = client_id
			self.reply(client_id, LOCK_SUCCESS)
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
			self.reply(client_id, UNLOCK_FAILURE)
			return UNLOCK_FAILURE

		if (self.lock_owners[x] != client_id):
			# unlock someone else's lock
			self.reply(client_id, UNLOCK_FAILURE)
			return UNLOCK_FAILURE

		if (self.lock_owners[x] == NO_OWNER):
			# unlock an available lock
			self.reply(client_id, UNLOCK_SUCCESS)
			return UNLOCK_SUCCESS


		if (self.lock_owners[x] == client_id
			and self.lock_states[x] == UNAVAILABLE):
			if not self.lock_wait_queues[x].empty():
				# someone else waiting for the lock
				# hand over lock
				waiting_client_id = self.lock_wait_queues[x].get()
				self.lock_owners[x] = waiting_client_id
				self.reply(waiting_client_id, LOCK_SUCCESS)
			else:
				# no one waiting for the lock
				self.lock_owners[x] = NO_OWNER
				self.lock_states[x] = AVAILABLE

			self.reply(client_id, UNLOCK_SUCCESS)
			return UNLOCK_SUCCESS

	def generate_response(self, command_id, result):
	    response_msg = {"type" : "response",
	                    "result" : {"command_id" : command_id,
	                                  "result_code" : result
	                                }}
	    return response_msg


	def reply(self, client_id, response_state):
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
		backlog = 5 
		maxbuf = 1024

		# create listening socket
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.bind((self.server_ip, self.port)) 
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
		    	client_conn.close()



if __name__ == "__main__":
    server = LockServer('127.0.0.1', 1111, 20)
    # terminate with Ctrl-C
    try:
        print "Lock Server started"
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)