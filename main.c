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
    PREP_STRING_TOO_LONG,
    PREP_UNRECOG_STATEMENT,
    PREP_NEG_ID
} PrepResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum
{
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS
} ExecuteResult;

typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

} Row;
typedef struct statement
{
    StatementType type;
    Row row_to_insert; // Only for insert statements

} Statement;

// we will be implementing heap file organization here
// define the compact representation of a row within a page

#define size_of_attr(Struct, Attr) sizeof(((Struct *)0)->Attr)
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
void serialize_tuple(Row *src, void *dest)
{
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
};

void deserialize_tuple(void *src, Row *dest)
{
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
};

// define a table struct that points to pages of rows and tracks how many rows there are
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

typedef struct
{
    uint32_t nrows;
    void *pages[TABLE_MAX_PAGES];
} Table;

// how to figure out where to read/write a particular row in memory?

void *row_slot(Table *table, uint32_t row_no)
{
    uint32_t page_no = row_no / ROWS_PER_PAGE;
    void *page = table->pages[page_no];
    if (page == NULL)
    {
        // allocate memory
        page = table->pages[page_no] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_no % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

InputBuffer *new_input_buffer()
{
    InputBuffer *IB = (InputBuffer *)malloc(sizeof(InputBuffer));
    IB->buffer = NULL;
    IB->buffer_length = 0;
    IB->input_length = 0;
    return IB;
}

PrepResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id = strtok(NULL, " ");
    char* uname = strtok(NULL, " ");
    char* mail = strtok(NULL, " ");
    if(id == NULL || id == NULL || uname == NULL) {
        return PREP_SYNTAX_ERROR; // no field is allowed to be null
    }
    int id_val = atoi(id);
    if (id_val < 0) {
        return PREP_NEG_ID;
    }
    if (strlen(uname) > COLUMN_USERNAME_SIZE) {
        return PREP_STRING_TOO_LONG;
    }
    if (strlen(mail) > COLUMN_EMAIL_SIZE) {
        return PREP_STRING_TOO_LONG;
    }
    statement->row_to_insert.id = id_val;
    strcpy(statement->row_to_insert.username, uname);
    strcpy(statement->row_to_insert.email, mail); // copying char* to char[]

    return PREP_SUCCESS;
}

PrepResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    if (strncmp(input_buffer->buffer, "INSERT", 6) == 0)
    { // INSERT cmd will be followed by data
        // statement->type = STATEMENT_INSERT;
        // int args_assigned = sscanf(
        //     input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id), statement->row_to_insert.username,
        //     statement->row_to_insert.email);
        // if (args_assigned < 3)
        // {
        //     return PREP_SYNTAX_ERROR;
        // }
        // return PREP_SUCCESS;
        return prepare_insert(input_buffer, statement);
    }
    if (strncmp(input_buffer->buffer, "SELECT", 6) == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREP_SUCCESS;
    }
    return PREP_UNRECOG_STATEMENT;
}

void print_row(Row *row)
{
    printf("%d %s %s\n", row->id, row->username, row->email);
}

ExecuteResult execute_select(Statement *statement, Table *table)
{
    Row row;
    for (uint32_t i = 0; i < table->nrows; i++)
    {
        deserialize_tuple(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement *statement, Table *table)
{
    if (table->nrows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);
    serialize_tuple(row_to_insert, row_slot(table, table->nrows));
    table->nrows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table)
{
    switch (statement->type)
    {
    case (STATEMENT_SELECT):
    {
        return execute_select(statement, table);
    }
    case (STATEMENT_INSERT):
    {
        return execute_insert(statement, table);
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

Table* create_table() {
    Table* table = (Table*)malloc(sizeof(Table));
    table->nrows = 0;
    for(uint32_t i=0; i<TABLE_MAX_PAGES; i++){
        table->pages[i]=NULL;
    }
    return table;

}

void free_table(Table* table) {
    for(uint32_t i=0; table->pages[i]; i++){
        free(table->pages[i]);
    }
    free(table);
}

MetaCmdResult do_meta_cmd(InputBuffer *input_buffer, Table* table)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_CMD_UNRECOG_CMD;
    }
}
// making the REPL

int main(int argc, char *argv[])
{
    // what is InputBuffer? a small wrapper around the state we need to store to
    // interact with getline()
    InputBuffer *input_buffer = new_input_buffer();
    Table* table=create_table();
    // to make a REPL, let's have an infinite loop that prints a prompt, gets a line of input, then
    // processes that line of input
    while (true)
    {
        print_prompt();
        read_input(input_buffer);
        // Non SQL Statements like .exit that start with a '.' are meta-cmds.
        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_cmd(input_buffer, table))
            {
            case (META_CMD_SUCCESS):
            {

                continue;
            }
            case (META_CMD_UNRECOG_CMD):
            {
                printf("Unrecognized command '%s' .\n", input_buffer->buffer);
            }
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        { // prepare_statement is our SQL Compiler
        case (PREP_SUCCESS):
        {
            break;
        }
        case (PREP_UNRECOG_STATEMENT):
        {
            printf("Unrecognized keyword at the start of '%s'.\n", input_buffer->buffer);
            continue;
        }
        case (PREP_SYNTAX_ERROR): 
        {
            printf("Syntax error detected.\n");
            continue;

        }
        case (PREP_STRING_TOO_LONG):
        {
            printf("Input fields are too long.\n");
            continue;
        }
        case (PREP_NEG_ID):
        {
            printf("Id can't be negative.\n");
            continue;
        }
        }
        switch(execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS): {
                printf("Successful execution.\n");
                break;
            }
            case (EXECUTE_TABLE_FULL): {
                printf("Table is full, no insertions possible.\n");
                break;

            }
        } // execute_statement is our SQL VM.
        printf("Executed.\n");
    }
    return 0;
}