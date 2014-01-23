#ifndef SHELL_H
#define SHELL_H

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <ctype.h>
#include <iostream>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>

using namespace std;

// shell program
// to handle commands delimited by |
void shell();

// strip off leading and trailing characters
char* stripwhite(char *string);

// parse and execute a command string
// parse the string to argument list argv
// call execvp to execute the command
// if success, do not return
// if error, must exit properly
void parse_execute_command(char *s);

// Parse and execute a whitespace-stripped line input from user
// line can contain multiple commands delimited by |
// First parses line to a list of commands
// Fork off a child and call helper to handle the list of commands
// must return
void parse_execute_line(char *s);

// Helper to execute a list of commands in parallel with pipes set up
// if only one command to execute, then no pipe set up;
// if more than one commands, set up pipe, 
// fork off child and recursively call itself
// on success, do not return
void parse_execute_line_helper(char *command_list[], int command_counts, int command_index);


#endif /* SHELL_H */