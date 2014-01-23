#ifndef SERVER_H
#define SERVER_H

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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <signal.h>

typedef struct {
	int client_fd;
	char *filepath;			// filepath requested
	int pipefd[2];					// pipe with main event loop
	void *mmap_addr;				// memory address of file requested
	int file_length;
	char *write_buf_position;		// write buffer current position
	int remaining_bytes_to_write;	// remaining bytes to write
} client_connection;

// stripwhite leading and trailing character
// do not allocate new memory
char* stripwhite(char *string);

// clean up client connection struct
void free_client_connection(client_connection *cc);

// worker function to be handled by ThreadPool
// argument is a pointer to client connection struct
// maps the file supplied in the client connection file_path to memory
void* read_file_return_mmap_address(void *argument);

// remove unwatched file descriptors
int remove_fd_from_fdsets(int i, int max_fd, fd_set *read_set, fd_set *write_set);
			
// clean up all file descriptors
void close_all_fds(int max_fd, fd_set &master_read_set, fd_set &master_write_set);


#endif /* 550SERVER_H */