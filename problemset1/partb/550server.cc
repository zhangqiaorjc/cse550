#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <ctype.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <errno.h>
#include <stdio.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <signal.h>

#include <iostream>

#include "ThreadPool.h"

#include <map>

#define handle_error(msg) \
           do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define PIPE_READ 0
#define PIPE_WRITE 1

/* TCP connection parameter */
#define MAXBUF 256
#define BACKLOG 10

/* ThreadPool parameter */
#define NUM_THREADS 10
#define MAX_TASK_QUEUE_SIZE 10

using namespace std;

char* stripwhite(char *string);

typedef struct {
	char filepath[MAXBUF];	// filepath requested
	int pipefd[2];	// pipe with main event loop
	void *mmap_addr;	// memory address of file requested
	int file_length;
	char *write_buf_position;	// write buffer current position
	int remaining_bytes_to_write;	// remaining bytes to write
} client_connection;

void free_client_connection(client_connection *cc) {
 	if (cc->mmap_addr) munmap(cc->mmap_addr, cc->file_length);
 	if (cc) delete cc;
}

void* read_file_return_mmap_address(char *filepath, int* file_length, void** mmap_addr) {		

	struct stat sb;

	int file_fd = open(filepath, O_RDONLY);
	if (file_fd != -1 && fstat(file_fd, &sb) != -1) {
		// open file successfully
		*file_length = sb.st_size;

		// mmap to memory
		*mmap_addr = mmap(NULL, *file_length, PROT_READ, MAP_PRIVATE, file_fd, 0);
		// can still be MAP_FAILED
	} else {
		*mmap_addr = MAP_FAILED;
	}
	return NULL;
}

void close_all_fds(int max_fd, fd_set &master_read_set, fd_set &master_write_set) {
	for (int i = 0; i <= max_fd; ++i) {
		if (FD_ISSET(i, &master_read_set) || FD_ISSET(i, &master_write_set))
			close(i);
	}
}

int main(int argc, char **argv) {

	if (argc < 3) {
		fprintf(stderr, "USAGE: ./550server HOST_IP HOST_PORT\n");
		exit(EXIT_FAILURE);
	}

	/* parse host IP and port */
	in_addr_t ip = inet_addr(argv[1]);
	if (ip == -1) {
		fprintf(stderr, "Host IP not in presentation format\n");
		exit(EXIT_FAILURE);
	}
	int port = atoi(argv[2]);

	/* create a TCP socket */
	int listening_fd;
	if ((listening_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		handle_error("cannot create socket");
	
	/* allow immediate reuse of the port */
	int sockoptval = 1;
	setsockopt(listening_fd, SOL_SOCKET, SO_REUSEADDR, &sockoptval, sizeof(int));

	/* set listening_fd to be non-blocking and all client sockets will be non-blocking */
	int fcntl_flags = fcntl(listening_fd, F_GETFL, 0);
	if (fcntl(listening_fd, F_SETFL, fcntl_flags | O_NONBLOCK) == -1)
		handle_error("fcntl");

	/* bind the socket to server address */
	struct sockaddr_in server_addr;
	memset((char*)&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port);
	server_addr.sin_addr.s_addr = ip;
	if (bind(listening_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
		handle_error("bind failed");

	/* listen on socket */
	if (listen(listening_fd, BACKLOG) < 0)
		handle_error("listen failed");
	
	/* create threadpool to handle async I/O */	
	ThreadPool tp(NUM_THREADS, MAX_TASK_QUEUE_SIZE);

	// initialize master set for select call
	fd_set master_read_set, working_read_set;
	fd_set master_write_set, working_write_set;
	int rc;	// select return val
	int max_fd = listening_fd;	// max sock descriptor
	FD_ZERO(&master_read_set);
	FD_ZERO(&master_write_set);
	FD_SET(listening_fd, &master_read_set);

	// timeout on select
	struct timeval timeout;
	timeout.tv_sec = 3 * 60;
	timeout.tv_usec = 0;

	// if client close connection during write, will raise SIGPIPE
	// so need to set SIGPIPE handler to ignore
	struct sigaction new_actn, old_actn;
	new_actn.sa_handler = SIG_IGN;
	sigemptyset(&new_actn.sa_mask);
	new_actn.sa_flags = 0;
	sigaction(SIGPIPE, &new_actn, &old_actn);

	// map from client_fd to client_connection struct
	map<int, client_connection*> client_connection_states;

	/* enter event handling loop */
	bool end_server = false;
	while (1) {
		// build working_set for select()
		memcpy(&working_read_set, &master_read_set, sizeof(master_read_set));
		memcpy(&working_write_set, &master_write_set, sizeof(master_write_set));
		
		if ((rc = select(max_fd + 1, &working_read_set, 
						&working_write_set, NULL,
						&timeout)) < 0) {
			perror("select");
			close_all_fds(max_fd, master_read_set, master_write_set);
			return 0;
		}

		if (rc == 0) {
			// timeout
		}

		int num_fds_ready = rc;
		for (int i = 0; i <= max_fd && num_fds_ready > 0; ++i) {

			if (FD_ISSET(i, &working_read_set)) {
				num_fds_ready--;	// one less fd to scan

				int client_fd;
				if (i == listening_fd) {
					// listening socket is ready
					// handle all pending connections
					do {
						socklen_t alen;
						struct sockaddr_in client_addr;
						client_fd = accept(listening_fd, (struct sockaddr *)&client_addr, &alen);
						if (client_fd < 0) {
							if (errno != EWOULDBLOCK && errno != EAGAIN) {
				        		perror("accept failed");
								// need to close fds
								close_all_fds(max_fd, master_read_set, master_write_set);
								return 0;
							}
							// all pending connections accepted; errno = EWOULDBLOCK or EAGAIN
						} else {
							// create client_connection_states entry
							client_connection_states[client_fd] = new client_connection();
					        // add new client_fd to master_read_set
					        // update max_fd
					        FD_SET(client_fd, &master_read_set);
					        if (client_fd > max_fd) max_fd = client_fd;
				    	}
				    } while (client_fd != -1);
			    } else {
			    	// a client fd is ready for reading
		 			char buffer[MAXBUF];
			   		memset(buffer, 0, MAXBUF);
					int read_nbytes = read(i, buffer, MAXBUF);

					// handle different read cases
					cout << "read_nbytes = " << read_nbytes << endl;
					if (read_nbytes == -1) {
						if (errno != EWOULDBLOCK && errno != EAGAIN) {
							// connection error
							perror("read from socket");
						}
					} else if (read_nbytes == 0) {
						// client orderly shutdown
						perror("client shutdown");
					} else {
						/* read filepath */
						buffer[read_nbytes] = '\0';
						char *filepath = stripwhite(buffer);

						/* fetch file to memory */
						strcpy(client_connection_states[i]->filepath, filepath);
						
						read_file_return_mmap_address(client_connection_states[i]->filepath, 
													&(client_connection_states[i]->file_length), 
													&(client_connection_states[i]->mmap_addr));
						client_connection_states[i]->write_buf_position 
							= (char *)client_connection_states[i]->mmap_addr;
						client_connection_states[i]->remaining_bytes_to_write
							= client_connection_states[i]->file_length;


						if (client_connection_states[i]->mmap_addr == MAP_FAILED) {
							perror("mmap");
							// delete client connection state since it will be shut down
							free_client_connection(client_connection_states[i]);
							client_connection_states.erase(i);
						} else {
							// wait for client to be writable in next select call
							// set client_fd in write_set
							FD_SET(i, &master_write_set);
						}
					}

					// finished reading from client_fd
					FD_CLR(i, &master_read_set);
					if (!FD_ISSET(i, &master_write_set)) {
						// if client_fd closed, close connection and update max_fd
						close(i);
						if (i == max_fd) {
							while (!FD_ISSET(max_fd, &master_read_set) && !FD_ISSET(max_fd, &master_write_set))
								max_fd--;
						}
					}
				}
			} else if (FD_ISSET(i, &working_write_set)) {
				num_fds_ready--;	// one less fd to scan

				// send file content to client
				// need to handle large file writes
				int write_nbytes = write(i, client_connection_states[i]->write_buf_position,
										 client_connection_states[i]->remaining_bytes_to_write);

				if (write_nbytes < 0)
					perror("write to socket");

				// update client write buffer position for next write
				client_connection_states[i]->write_buf_position += write_nbytes;
				client_connection_states[i]->remaining_bytes_to_write -= write_nbytes;
				// if finished writing the entire file
				if (client_connection_states[i]->remaining_bytes_to_write == 0) {
					// delete client connection state since it will be shut down
					free_client_connection(client_connection_states[i]);
					client_connection_states.erase(i);
					// close client connection
					close(i);
					FD_CLR(i, &master_write_set);

					// update max_fd
					if (i == max_fd) {
						while (!FD_ISSET(max_fd, &master_read_set) && !FD_ISSET(max_fd, &master_write_set))
							max_fd--;
					}
				}
			}
		}
	}
	printf("terminate server.\n");
	close_all_fds(max_fd, master_read_set, master_write_set);
	return 0;
}

// remove leading and trailing whitespace
char* stripwhite(char *string) {
	char *s, *t;
	for (s = string; iswspace(*s); ++s) {}
	for (t = string + strlen(string) - 1; iswspace(*t); --t) {}
	*(++t) = '\0';

	return s;
}

