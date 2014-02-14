## __Distributed Lock Service Using Multi-Paxos__

### __Architecture Overview: Different Actors and Their Roles__

I implemented the multi-instance Paxos described in the paper "Paxos Made Moderately Complex" with some modifications and optimizations. The time diagram in Figure 1 (Fig. 5 in the original paper) shows clearly the actors in the system and the different kinds of messages being sent and the sequence they are sent.

![Alt text](http://homes.cs.washington.edu/~qiao/misc/paxos_time_diagram.png "Figure 1: time diagram ")

* __client__ (lock client)

	- client issues lock/unlock request to the replicas (lock servers) in the form of
	(client_id, command_id, op) where client_id uniquely identifies the client, and
	command_id uniquely identifies the order of per-client request.
	op is a string, e.g. "lock 1" or "unlock 0" that specifies the lock number
	to acquire or release.

	- client receives response from lock servers in the form of (command_id, result_code)
	where command_id identifies the order of the request, result_code (o or 1) to indicate
	the success or failure of the request at the lock server.

* __replica__ (lock server)
	
	- replica serves two roles: 

		- it is the state machine that models the lock server, where
	we keep server states such as lock_states (lock available or not available), lock_owners
	(the client_id of the client id that holds the lock) and lock_wait_queue (that has a queue
	of client_ids of the blocked clients for each lock).

		- it also serves as the proposer for Paxos. it receives the request from the clients, and
	chooses the lowest available slot number for that request before proposing it to the leader.
	The proposal has the form (slot_number, proposal_value) where the slot number is the sequence
	number the server proposes for the client request and proposal_value is exactly the client
	request.

	- replica also receives decision messages in the form of (slot_number, proposal_value). The
	slot_num dictates the sequence number of the client request that is being decided by the Paxos
	algorithm (the sequence number is supposed to agree across all the replicas for the client request
	in order to maintain the invariant that all replicas execute the client requests in the same order
	and therefore always maintain consistent state with each other)
	
* __leader(s)__

	- the leader initiates phase 1 and phase 2 of the Paxos algorithm for each slot number.
	There can be multiple leaders in the system. They each have a boolean self.active that identifies
	if it is the active leader at that moment in time. A leader starts being inactive.

	- each leader keeps a ballot number in the form (ballot_num, leader_id). This tuple format
	ensures that no two leaders ever use the same ballot number, and the same leader always issue
	increasing ballot numbers by incrementing the first field of the tuple. An extra advantage
	of this tuple format is that when a leader increments the first field of the tuple, its ballot
	number immediately trumps those from the other leaders.

	- each leader spawns a scout (multithreaded in my implementation) to conduct phase 1 to become
	the active leader:
		- __scout__
			- sends a p1a message to all acceptors and wait for a quorum of p1b replies from
			acceptors. Once the scout gets a quorum of p1b replies with the same ballot number as the leader
			that spawns it, the scout sends an "adopted" message to the leader, informing the leader that
			a majority of acceptors have accepted its p1a message. Otherwise, if the scout receives any p1b
			message saying that an acceptor has prepared to a larger ballot number, then the scout
			immediately informs the leader that it is pre-empted by another active leader, and therefore
			fails the phase 1 prepare stage.

		- if the leader receives "pre-empted" message from its scout, it knows that the acceptors have responded
		to a larger ballot number, therefore it will not succeed in getting its p2a message accepted, so 
		it either immediately spawns another scout with an incremented ballot number (which may result in liveness issues since two leaders can alternately trying to pre-empt each other) or recognize the active leader and monitor it by pinging periodically and wait for timeouts. Once the inactive leader timesout on pinging the active leader, the inactive leader spawns a scout for p1a. In some way, this implementes a form of leader election.

		- if the leader receives an "adopted" message, it becomes the active leader and is able to go on to phase 2

	- when the leader receives a "propose" message from a replica, it adds the proposal (slot_num, proposal_value) to a list of proposals it has seen. If the leader is active, it will spawn a commander to complete the phase 2 of Paxos
		- __commander__
			- sends p2a to all acceptors and wait for a quorum of p2b replies from
			acceptors. Once the commander gets a quorum of p2b replies with the same ballot number as the leader
			that spawns it, the commander sends an "decision" message to all the replicas, informing the replicas that (slot_num, proposal_value) has been decided by Paxos. If the commander, however, receives a p2b reply with a higher ballot number than that of the leader that spawns it, it knows 
			that some other leader has in the meantime become active independently and prepared the acceptors.
			The commander needs to pre-empt the leader as a consequence.

* __acceptors__
	
	- the acceptor sits in an listening loop to respond to p1a from scouts and p2a from commanders
	- the acceptor remembers the highest ballot number that it has prepared to, and also the list of
	accepted proposal values (I only keep the proposal_values of the largest ballot number for each
	slot_num since that is enough for the leader to figure out what proposal_value to choose on receiving
	a p2b reply)


### __Known Issues and Their solutions if any__

* Once lock server performs client request, it sends its response to the client. However, the
response message can get lost and the client would block. The solution is allow client to
timeout on receiving the responses, and resend its request on timeout. The lock server needs
to record the last response that it has sent to the client and on receiving any request, first
check if the request has already been fulfilled.

* Since client sends requests to all replicas and those requests can be reordered. The replicas
can end up assigning different slot numbers for the same request. And those conflicting proposals
will be sent to the leader. So if the leader is not careful, it can end up deciding up different
proposal value for the same slot number, which has occurred in my program initially. As a solution,
the leader on receiving a proposal from a replica, checks the slot number and the proposal value
of the new proposal against all the proposals it has seen so far, and the leader would discard
the new proposal if it has seen a proposal with the same slot number or the same proposal value.
This has in practice solved the problem.

* Decision messags from commanders get lost on their way to the lock servers. This
causes the lock servers to stuck since the leaders do not re-send decision messages. I implemented
a collaborative query protocol where lock servers on listen socket timeout, sends queries
to all other lock servers for any decisions reached on its current slot number. This has
in practice solved the problem. But in the worst case, all lock servers could have missed
the decision messages, in which case, the lock servers should query the leaders who are the 
authority on what decisions have been made so far.

* Inactive leaders ping the active leaders in order to monitor for failure. The pinging is done
by creating a socket and connect with active leader and then sleeps for 3 seconds after closing
the connection. However, in practice, those connection requests block the active leader from
processing other messages, in effect, DoS attacked the active leader. So those code are commented
out. The liveness condition seems to be better when the leaders contend relative to "pinging".
Another solution I tried is random backoff when a leader gets preempted. This has worked somewhat
in practice. The final solution I decided on is for the active leader to send out periodic 
keepalive message when it's idle to all the other leaders. An inactive leader keeps track of when the 
last time it heard from the active leader and if that exceeds a threshold, the inactive leader
sends out scout to elect itself as the leader. This solution has the advantage that the active leader
gets to decide when to send out keepalive, so it can still spend most of its time processing normal
messages

* Using dictionary to represent messages and then serialize with JSON is very verbose
and has high performance overhead, but it does make the debugging much easier since
the messages are in plaintext and can be printed in logs. The alternative is protobuf
which does binary encoding or compress messages before sending them. However, the alternatives
make the messages harder to display and debug.


### __Implementation Details__

* The implementation is in Python for brevity and readability.

* Assumptions about acquiring and releasing locks.
	- When a lock is released, it is hand over to the next waiting client.
	- A lock can be acquired by the client repeatedly without failure, and a single unlock
call releases the lock. 
	- An available lock can be unlocked by any client without failure.
	- A client cannot unlock someone else's lock.

* Actor network addresses and port numbers are specified in a central configuration file
"paxos_group_config.json" that is loaded at each program for easy modification and lookup

* Messages are Python dictionaries, and they are serialized using JSON
for network communication.

* TCP connections are open and immediately closed after sending a message. 
Separate listening socket is created to receive messages in a event loop.

* none of my actors implement recovery, that is if any actor crashes, it cannot rejoin the
system; in order to implement recovery, all internal states need to be written to disk, and
be loaded on starting the actors

* detailed log messages are printed on all relevant operations for the ease of debugging