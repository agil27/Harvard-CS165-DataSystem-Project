/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}


DbOperator* parse_shutdown(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
    dbo->type = SHUTDOWN;
    dbo->operator_fields.shutdown_operator.save = 1;
    return dbo;
}


DbOperator* parse_load(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(\"", 2) == 0) {
        query_command += 2;
        char** command_index = &query_command;
        DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
        dbo->type = LOAD;
        token = next_token(command_index, &send_message->status);
        int token_len = strlen(token);
        if (token[token_len - 1] == ')' && token[token_len - 2] == '\"') {
            token[token_len - 2] = '\0';
        } else {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        }
        strcpy(dbo->operator_fields.load_operator.path, token);
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


DbOperator* parse_print(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = PRINT;
        dbo->operator_fields.print_operator.num_name = 0;
        token = next_token(command_index, &send_message->status);
        while (token != NULL) {
            int token_len = strlen(token);
            if (token[token_len - 1] == ')') {
                token[token_len - 1] = '\0';
            }
            strcpy(
                dbo->operator_fields.print_operator.names[
                    dbo->operator_fields.print_operator.num_name++
                ],
                token
            );
            token = next_token(command_index, &send_message->status);
        }
        // check that we received the correct number of input values
        if (dbo->operator_fields.print_operator.num_name == 0) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


DbOperator* parse_fetch(char* fetch_arguments, message* send_message, char* res_name, ClientContext* context) {
    char* query_commands = fetch_arguments;
    if (strncmp(query_commands, "(", 1) == 0) {
        query_commands++;
        char** fetch_arguments_index = &query_commands;
        char* fullname = next_token(fetch_arguments_index, &send_message);
        Column* column = NULL;
        Table* table = NULL;
        lookup_column_table(fullname, &column, &table);
        if (column == NULL || table == NULL) {
            send_message = OBJECT_NOT_FOUND;
            return NULL;
        }
        char* select_res_name = next_token(fetch_arguments_index, &send_message);
        int select_res_len = strlen(select_res_name);
        if (select_res_name[select_res_len - 1] != ')') {
            send_message = UNKNOWN_COMMAND;
            return NULL;
        }
        select_res_name[select_res_len - 1] = '\0';
        SelectResult select_res = get_select_res(context, select_res_name);

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        strcpy(dbo->operator_fields.fetch_operator.res_name, res_name);
        dbo->operator_fields.fetch_operator.table = table;
        dbo->operator_fields.fetch_operator.column = column;
        dbo->operator_fields.fetch_operator.select_result = select_res;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


long null_atoi(const char* str, int low) {
    if (strncmp(str, "null", 4) == 0) {
        if (low) {
            return LONG_MAX;
        } else {
            return LONG_MIN;
        }
    } else {
        return atoi(str);
    }
}


DbOperator* parse_select(char* select_arguments, message* send_message, char* res_name, ClientContext* context) {
    char* query_commands = select_arguments;
    if (strncmp(query_commands, "(", 1) == 0) {
        query_commands++;
        char** insert_argument_index = &query_commands;
        char* fullname = next_token(insert_argument_index, &send_message);
        char* fetch_name = "";
        
        // check if it's column or select result
        char* after_dot = strrchr(fullname, '.');
        if (after_dot == NULL) {
            fetch_name = next_token(insert_argument_index, &send_message);
        }

        // build comparator
        long min_val = null_atoi(next_token(insert_argument_index, &send_message), 1);
        long max_val = null_atoi(next_token(insert_argument_index, &send_message), 0);
        Comparator cmp;
        cmp.p_high = max_val;
        cmp.p_low = min_val;

        // case 1: column selection
        if (after_dot != NULL) {
            Column* column = NULL;
            Table* table = NULL;
            lookup_column_table(fullname, &column, &table);
            if (column == NULL || table == NULL) {
                send_message = OBJECT_NOT_FOUND;
                return NULL;
            }
            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = SELECT;
            dbo->operator_fields.select_operator.select_type = COLUMN_SELECT;
            strcpy(dbo->operator_fields.select_operator.select_operator_field.cso.res_name, res_name);
            dbo->operator_fields.select_operator.select_operator_field.cso.table = table;
            dbo->operator_fields.select_operator.select_operator_field.cso.column = column;
            dbo->operator_fields.select_operator.select_operator_field.cso.cmp = cmp;
            return dbo;
        } else {
            // case 2: fetch result selection
            FetchResult* fetch_result = get_chandle(context, fetch_name)->generalized_column.column_pointer.result;
            SelectResult select_result = get_select_res(context, fullname);
            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = SELECT;
            dbo->operator_fields.select_operator.select_type = FETCH_SELECT;
            strcpy(dbo->operator_fields.select_operator.select_operator_field.fso.res_name, res_name);
            dbo->operator_fields.select_operator.select_operator_field.fso.select_res = select_result;
            dbo->operator_fields.select_operator.select_operator_field.fso.fetch_res = fetch_result;
            dbo->operator_fields.select_operator.select_operator_field.fso.cmp = cmp;
            return dbo;
        }
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


DbOperator* parse_create_col(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* fullname = next_token(create_arguments_index, &status);
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }

    col_name = trim_quotes(col_name);
    int last_char = strlen(fullname) - 1;
    if (fullname[last_char] != ')') {
        return NULL;
    }
    fullname[last_char] = '\0';
    
    // lookup the table and make sure it exists. 
    Table* table = lookup_table(fullname);
    if (table == NULL) {
        cs165_log(stdout, "query unsupported. Bad table name");
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.table = table;
    dbo->operator_fields.create_operator.db = current_db;
    return dbo;
}


DbOperator* parse_create_index(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* index_type = next_token(create_arguments_index, &status);
    char* clustered = next_token(create_arguments_index, &status);
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }

    int last_char = strlen(clustered) - 1;
    if (clustered[last_char] != ')') {
        return NULL;
    }
    clustered[last_char] = '\0';
    ClusterType clusterType;
    if (strcmp(clustered, "clustered") == 0) {
        clusterType = CLUSTERED;
    } else {
        clusterType = UNCLUSTERED;
    }

    IndexType indexType;
    if (strcmp(index_type, "btree") == 0) {
        indexType = BTREE;
    } else {
        indexType = SORTED;
    }

    // lookup the table and make sure it exists. 
    Column* column = NULL;
    Table* table = NULL;
    lookup_column_table(col_name, &column, &table);

    if (table == NULL) {
        cs165_log(stdout, "query unsupported. Bad table name");
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _INDEX;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.table = table;
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.column = column;
    dbo->operator_fields.create_operator.clusterType = clusterType;
    dbo->operator_fields.create_operator.indexType = indexType;
    return dbo;
}


DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments) {
    message_status mes_status;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy);
            } else if (strcmp(token, "idx") == 0) {
                dbo = parse_create_index(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = NULL;
        dbo = (DbOperator*)malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = (int*)malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


DbOperator* parse_join(char* query_command, message* send_message, char* handle, ClientContext* context) {
    char* token = NULL;
    char** handle_index = &handle;
    char* name1 = next_token(handle_index, &send_message->status);
    char* name2 = next_token(handle_index, &send_message->status);
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* f1n = next_token(command_index, &send_message->status);
        char* p1n = next_token(command_index, &send_message->status);
        char* f2n = next_token(command_index, &send_message->status);
        char* p2n = next_token(command_index, &send_message->status);
        FetchResult* f1 = get_chandle(context, f1n)->generalized_column.column_pointer.result;
        FetchResult* f2 = get_chandle(context, f2n)->generalized_column.column_pointer.result;
        SelectResult p1 = get_select_res(context, p1n);
        SelectResult p2 = get_select_res(context, p2n);

        char* join_type = next_token(command_index, &send_message->status);
        int join_type_len = strlen(join_type);
        if (join_type[join_type_len - 1] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        join_type[join_type_len - 1] = '\0';
        JoinType jtype;
        if (strcmp(join_type, "hash") == 0) {
            jtype = HASH;
        } else if (strcmp(join_type, "nested-loop") == 0) {
            jtype = NESTED_LOOP;
        } else {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
        dbo->type = JOIN;
        dbo->operator_fields.join_operator.jtype = jtype;
        dbo->operator_fields.join_operator.f1.data = f1->data;
        strcpy(dbo->operator_fields.join_operator.f1.fetch_name, f1->fetch_name);
        dbo->operator_fields.join_operator.f1.num_rows = f1->num_rows;
        dbo->operator_fields.join_operator.f2.data = f2->data;
        strcpy(dbo->operator_fields.join_operator.f2.fetch_name, f2->fetch_name);
        dbo->operator_fields.join_operator.f2.num_rows = f2->num_rows;
        dbo->operator_fields.join_operator.p1.id = p1.id;
        dbo->operator_fields.join_operator.p1.num_ids = p1.num_ids;
        strcpy(dbo->operator_fields.join_operator.p1.select_name, p1.select_name);
        dbo->operator_fields.join_operator.p2.id = p2.id;
        dbo->operator_fields.join_operator.p2.num_ids = p2.num_ids;
        strcpy(dbo->operator_fields.join_operator.p2.select_name, p2.select_name);
        strcpy(dbo->operator_fields.join_operator.r1n, name1);
        strcpy(dbo->operator_fields.join_operator.r2n, name2);
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_agg(char* query_command, message* send_message, char* handle, AggType type) {
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* chandle_name = next_token(command_index, &send_message->status);
        int chandle_len = strlen(chandle_name);
        if (chandle_name[chandle_len - 1] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        chandle_name[chandle_len - 1] = '\0';
        // make insert operator. 
        DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
        dbo->type = AGG;
        dbo->operator_fields.agg_operator.type = type;
        strcpy(dbo->operator_fields.agg_operator.handle, chandle_name);
        strcpy(dbo->operator_fields.agg_operator.res_name, handle);
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_avg(char* query_command, message* send_message, char* handle) {
    return parse_agg(query_command, send_message, handle, AVG);
}

DbOperator* parse_sum(char* query_command, message* send_message, char* handle) {
    return parse_agg(query_command, send_message, handle, SUM);
}

DbOperator* parse_min(char* query_command, message* send_message, char* handle) {
    return parse_agg(query_command, send_message, handle, MIN);
}

DbOperator* parse_max(char* query_command, message* send_message, char* handle) {
    return parse_agg(query_command, send_message, handle, MAX);
}

DbOperator* parse_math(char* query_command, message* send_message, char* handle, MathType type) {
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* first = next_token(command_index, &send_message->status);
        char* second = next_token(command_index, &send_message->status);
        int second_len = strlen(second);
        if (second[second_len - 1] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        second[second_len - 1] = '\0';

        // make insert operator. 
        DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
        dbo->type = MATH;
        dbo->operator_fields.math_operator.type = type;
        strcpy(dbo->operator_fields.math_operator.handle1, first);
        strcpy(dbo->operator_fields.math_operator.handle2, second);
        strcpy(dbo->operator_fields.math_operator.res_name, handle);
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_add(char* query_command, message* send_message, char* handle) {
    return parse_math(query_command, send_message, handle, ADD);
}

DbOperator* parse_sub(char* query_command, message* send_message, char* handle) {
    return parse_math(query_command, send_message, handle, SUB);
}

DbOperator* parse_batch_query(char* query_command, message* send_message, char* handle, ClientContext* context) {
    context->batch = 1;
    DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
    dbo->type = BATCH_QUERY;
    return dbo;
}

DbOperator* parse_batch_exec(char* query_command, message* send_message, char* handle, ClientContext* context) {
    context->batch = 0;
    DbOperator* dbo = (DbOperator*)malloc(sizeof(DbOperator));
    dbo->type = BATCH_EXEC;
    return dbo;
}


/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }
    cs165_log(stdout, "QUERY: %s\n", query_command);
    query_command = trim_whitespace(query_command);
    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command);
        if(dbo == NULL){
            send_message->status = INCORRECT_FORMAT;
        }
        else{
            send_message->status = OK_DONE;
        }
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, send_message, handle, context);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, &send_message, handle, context);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, &send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, &send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        query_command += 8;
        dbo = parse_shutdown(query_command, &send_message);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_avg(query_command, &send_message, handle);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_sum(query_command, &send_message, handle);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_min(query_command, &send_message, handle);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_max(query_command, &send_message, handle);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_add(query_command, &send_message, handle);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_sub(query_command, &send_message, handle);
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        dbo = parse_batch_query(query_command, &send_message, handle, context);
    } else if (strncmp(query_command, "batch_execute", 13) == 0) { 
        dbo = parse_batch_exec(query_command, &send_message, handle, context);
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4;
        dbo = parse_join(query_command, &send_message, handle, context);
    }
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
