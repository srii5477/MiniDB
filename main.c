#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct IB {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
    InputBuffer* IB=(InputBuffer*)malloc(sizeof(InputBuffer));
    IB->buffer=NULL;
    IB->buffer_length=0;
    IB->input_length=0;
    return IB;
}

void print_prompt(){
    printf("minidb > ");
}

void read_input(InputBuffer* input_buffer) {
    char str[10000];
    scanf("%s", str);
    input_buffer->buffer=str;
}
// making the REPL

int main(int argc, char* argv[]) {
    // what is InputBuffer? a small wrapper around the state we need to store to 
    // interact with getline()
    InputBuffer* input_buffer = new_input_buffer();
    // to make a REPL, let's have an infinite loop that prints a prompt, gets a line of input, then
    // processes that line of input
    while(true) {
        print_prompt();
        read_input(input_buffer);
        if(strcmp(input_buffer->buffer, ".exit")==0) {
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command '%s' .\n", input_buffer->buffer);
        }
    }
    return 0;
}