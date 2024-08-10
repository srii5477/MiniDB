#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <conio.h>
#include "header.h"
#include <stdint.h>
#define COLUMN_USERNAME_SIZE 50
#define COLUMN_EMAIL_SIZE 255


typedef struct IB
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum
{
    META_CMD_SUCCESS,
    META_CMD_UNRECOG_CMD
} MetaCmdResult;

typedef enum
{
    PREP_SUCCESS,
    PREP_SYNTAX_ERROR,
    PREP_UNRECOG_STATEMENT
} PrepResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
    
} Row;
typedef struct statement
{
    StatementType type;
    Row row_to_insert; // Only for insert statements

} Statement;

// we will be implementing heap file organization here
// define the compact representation of a row within a page

#define size_of_attr(Struct, Attr) sizeof(((Struct*)0)->Attr)
// sizeof(((Struct*)0)->Attr): 0 here is a null pointer constant cast to a pointer of type Struct*,
// so we get the size of the struct attribute without having to make an object

const uint32_t ID_SIZE = size_of_attr(Row, id);
const uint32_t USERNAME_SIZE = size_of_attr(Row, username);
const uint32_t EMAIL_SIZE = size_of_attr(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// code to convert to and from the compressed tuple format
void serialize_tuple(Row* src, void* dest) {
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
};

void deserialize_tuple(void* src, Row* dest) {
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
};

// define a table struct that points to pages of rows and tracks how many rows there are
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

typedef struct {
    uint32_t nrows;
    void* pages[TABLE_MAX_PAGES];
} Table;

InputBuffer *new_input_buffer()
{
    InputBuffer *IB = (InputBuffer *)malloc(sizeof(InputBuffer));
    IB->buffer = NULL;
    IB->buffer_length = 0;
    IB->input_length = 0;
    return IB;
}

MetaCmdResult do_meta_cmd(char* buffer)
{
    if (strcmp(buffer, ".exit") == 0)
    {
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_CMD_UNRECOG_CMD;
    }
}

PrepResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if(strncmp(input_buffer->buffer, "INSERT", 6) == 0) { // INSERT cmd will be followed by data
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(
            input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id), statement->row_to_insert.username, 
            statement->row_to_insert.email
        );
        if(args_assigned < 3) {
            return PREP_SYNTAX_ERROR;
        }
        return PREP_SUCCESS;
    }
    if(strncmp(input_buffer->buffer, "SELECT", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREP_SUCCESS;
    } 
    return PREP_UNRECOG_STATEMENT;
}

void execute_statement(Statement statement) {
    switch(statement.type) {
        case(STATEMENT_SELECT): {
            printf("This is where we'll have to run a SELECT query.\n");
            break;
        }
        case(STATEMENT_INSERT): {
            printf("This is where we'll have to run an INSERT query.\n");
            break;
        }
        // error handling who?
    }
}

void print_prompt()
{
    printf("minidb > ");
}



void read_input(InputBuffer *input_buffer)
{
    // getline returns number of bytes read
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0)
    {
        printf("Error reading input\n");
        exit(EXIT_SUCCESS);
    }

    // don't count the trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[input_buffer->input_length] = 0;
}

void close_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}
// making the REPL

int main(int argc, char *argv[])
{
    // what is InputBuffer? a small wrapper around the state we need to store to
    // interact with getline()
    InputBuffer *input_buffer = new_input_buffer();
    // to make a REPL, let's have an infinite loop that prints a prompt, gets a line of input, then
    // processes that line of input
    while (true)
    {
        print_prompt();
        read_input(input_buffer);
        // Non SQL Statements like .exit that start with a '.' are meta-cmds.
        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_cmd(input_buffer->buffer))
            {
                case (META_CMD_SUCCESS): {
                    continue;
                }
                case (META_CMD_UNRECOG_CMD): {
                    printf("Unrecognized command '%s' .\n", input_buffer->buffer);
                }
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) { // prepare_statement is our SQL Compiler
            case (PREP_SUCCESS): {
                break;
            }
            case (PREP_UNRECOG_STATEMENT): {
                printf("Unrecognized keyword at the start of '%s'.\n", input_buffer->buffer);
                continue;
            }
        }
        execute_statement(statement); // execute_statement is our SQL VM.
        printf("Executed.\n");
    }
    return 0;
}