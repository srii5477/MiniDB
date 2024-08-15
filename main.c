#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <conio.h>
#include "header.h"
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

// pager is an abstraction for a part of the DBMS that checks whether
// the requested page is in the buffer pool (which we'll implement as the pager's cache itself)
// if it is not there it fetches it from the disk.

typedef struct
{
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES]; // this is the in-memory cache
} Pager;

typedef struct
{
    uint32_t nrows;
    Pager* pager;
} Table;

void* get_page(Pager* pager, uint32_t page_no) {
    // deal with cache miss, out-of-bounds page access etc. here
    
    /*
     If the requested page lies outside the bounds of the file, we know it should be blank, 
     so we just allocate some memory and return it. The page will be added to the file when 
     we flush the cache to disk later.
    */
   if(page_no > TABLE_MAX_PAGES) {
        printf("The page number %d trying to be accessed is out of bounds. MAX: %d.\n", page_no, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
   }
   // cache miss: allocate memory and load page from disk
   if (pager->pages[page_no] == NULL) {
        void* page = malloc(PAGE_SIZE);
        uint32_t npages = pager->file_length/PAGE_SIZE;
        if(pager->file_length % PAGE_SIZE > 0) {
            // save a partial page
            npages++;
        }    
        if(page_no <= npages) {
            lseek(pager->file_descriptor, page_no*PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_no] = page;
   }
   return pager->pages[page_no];
}
// how to figure out where to read/write a particular row in memory?

void *row_slot(Table *table, uint32_t row_no)
{
    uint32_t page_no = row_no / ROWS_PER_PAGE;
    void* page = get_page(table->pager, page_no);
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

PrepResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id = strtok(NULL, " ");
    char *uname = strtok(NULL, " ");
    char *mail = strtok(NULL, " ");
    if (id == NULL || id == NULL || uname == NULL)
    {
        return PREP_SYNTAX_ERROR; // no field is allowed to be null
    }
    int id_val = atoi(id);
    if (id_val < 0)
    {
        return PREP_NEG_ID;
    }
    if (strlen(uname) > COLUMN_USERNAME_SIZE)
    {
        return PREP_STRING_TOO_LONG;
    }
    if (strlen(mail) > COLUMN_EMAIL_SIZE)
    {
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

Pager* open_file(const char* file_name)
{
    int fd = open(
        file_name, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR
    );
    if(fd == -1) {
        printf("Unable to open the database file. Unresolvable error.\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = (Pager*)malloc(sizeof(pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    // initialize cache blocks to NULL
    for(uint32_t i=0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i]=NULL;
    }
    return pager;

}
Table *open_db(const char* file_name) // renamed because we have to open our db file, initialize our cache as well as create a table
{
    Pager* pager = open_file(file_name); 
    Table *table = (Table *)malloc(sizeof(Table));
    table->pager = pager;
    uint32_t num_rows = pager->file_length/ROW_SIZE;
    table->nrows = num_rows; // file is a table
    return table;
}


void flush(Pager* pager, int page_no, int mem_to_delete) {
    if(pager->pages[page_no] == NULL) {
        printf("Can't flush a null page.\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor, page_no*PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_no], mem_to_delete);
    if (bytes_written) {
        printf("Error flushing to disk: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    //exit(EXIT_SUCCESS);

}

void close_db(Table* table) {
    // things to be done here:
    // flush page cache to disk
    // close db file
    // free memory for Pager and Table structures
    Pager* pager = table->pager;
    uint32_t full_pages = table->nrows/ROWS_PER_PAGE;
    for(uint32_t i = 0; i < table->nrows/ROWS_PER_PAGE; i++) {
        if(pager->pages[i]!=NULL) { // implement dirty page handling mechanism here later
            flush(pager, i, PAGE_SIZE);
            free(pager->pages[i]); // dangling ptr
            pager->pages[i]=NULL; // set to null
        }
    }
    // there may be a partial page to write to the end of the file
    uint32_t additional_pages = table->nrows % ROWS_PER_PAGE;
    if (additional_pages > 0) {
        if(pager->pages[full_pages]!=NULL) {
            flush(pager, full_pages, ROW_SIZE*additional_pages);
            free(pager->pages[full_pages]);
            pager->pages[full_pages]=NULL;
        }
    }
    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error in closing db file: %d \n", errno);
        exit(EXIT_FAILURE);
    }
    free(pager);
    free(table);
}

MetaCmdResult do_meta_cmd(InputBuffer *input_buffer, Table *table)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_db(table);
        close_input_buffer(input_buffer);
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
    
    if  (argc < 2) {
        printf("Provide a filename.\n");
        exit(EXIT_FAILURE);
    }
    char* file_name = argv[1];
    Table* table = open_db(file_name);
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
        switch (execute_statement(&statement, table))
        {
        case (EXECUTE_SUCCESS):
        {
            printf("Successful execution.\n");
            break;
        }
        case (EXECUTE_TABLE_FULL):
        {
            printf("Table is full, no insertions possible.\n");
            break;
        }
        } // execute_statement is our SQL VM.
        printf("Executed.\n");
    }
    return 0;
}