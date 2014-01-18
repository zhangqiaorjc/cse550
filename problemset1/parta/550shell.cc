#include "550shell.h"

#define PIPE_READ 0
#define PIPE_WRITE 1

int main(int argc, char **argv) {
	shell();
	exit(EXIT_SUCCESS);
}

void shell() {

	/* handle user input loop */
	
	char *line, *s;
	while (true) {

		// print shell prompt "550shell$" and read new line
		line = readline("550shell$ ");
		// if EOF and empty line, shell terminates
		if (!line) break;

		// remove leading and trailing whitespace
		s = stripwhite(line);
		if (*s) {
			add_history(line);
			parse_execute_line(s);
		}

		// readline malloc line so must free line
		free(line);
	}
}

void parse_execute_line(char *s) {

	/* parse line to array of commands */

	char *command;
	char *command_list[strlen(s) / 2];	// at most strlen(s) / 2 arguments
	int command_counts;
	for (command_counts = 0, command = strtok(s, "|"); command;
		command = strtok(NULL, "|"), command_counts++) {
		command_list[command_counts] = command;
	}
	command_list[command_counts] = NULL;

	// if no commands found
	if (command_counts == 0) return;
	
	/* execute list of arguments */
		
	// fork off a child
	pid_t cpid;
	switch (cpid = fork()) {
	case -1:
		// fork error
		perror("fork");
		exit(EXIT_FAILURE);
	
	case 0:
		parse_execute_line_helper(command_list, command_counts, 0);

	default:
		// parent process
		int status;
		if (waitpid(cpid, &status, 0) == -1) {
            //perror("waitpid");
            exit(EXIT_FAILURE);
        }
	}
	
}

void parse_execute_line_helper(char *command_list[], 
						int command_counts, 
						int command_index) {
	
	if (command_index == command_counts - 1) {
		// if last command
		parse_execute_command(command_list[command_index]);
		// if command not found
		return;
	}

	// pipe communication
	int pipefd[2];
	// if more than one command to run
	if (pipe(pipefd) == -1) {
		// pipe creation error
		perror("pipe");
		exit(EXIT_FAILURE);
	}

	// fork off a child
	pid_t cpid;
	switch (cpid = fork()) {
	case -1:
		// fork error
		perror("fork");
		exit(EXIT_FAILURE);
	
	case 0:
		// child process
		// redirect STDOUT to pipe write end
		if (dup2(pipefd[PIPE_WRITE], STDOUT_FILENO) == -1) {
			perror("dup2");
			exit(EXIT_FAILURE);
		}
		// close off pipe read end
		close(pipefd[PIPE_READ]);
		parse_execute_command(command_list[command_index]);
		// must exit otherwise next command waits for input indefinitely
		exit(EXIT_FAILURE);
	
	default:
		// parent process
		// redirect STDIN to pipe read end
		if (dup2(pipefd[PIPE_READ], STDIN_FILENO) == -1) {
			perror("dup2");
			exit(EXIT_FAILURE);
		}
		// close off pipe write end
		close(pipefd[PIPE_WRITE]);
		parse_execute_line_helper(command_list, command_counts, command_index + 1);
	}
}


void parse_execute_command(char *s) {
	/* parse argument */
	char *token;
	// at most strlen(s) / 2 arguments
	char *argv[strlen(s) / 2];
	int argc;
	for (argc = 0, token = strtok(s, " "); token;
		token = strtok(NULL, " "), argc++) {
		argv[argc] = token;
	}
	argv[argc] = NULL;

	/* execute command */
	// automatically search in PATH variable
	if (execvp(argv[0], argv) == -1) {
		// if command not found
		fprintf(stderr, "-bash: %s: command not found\n", argv[0]);
	}
}


// remove leading and trailing whitespace
char* stripwhite(char *string) {
	char *s, *t;
	// remove leading whitespace
	for (s = string; iswspace(*s); ++s) {}
	// remove trailing whitespace
	for (t = string + strlen(string) - 1; iswspace(*t); --t) {}
	*(++t) = '\0';

	return s;
}

