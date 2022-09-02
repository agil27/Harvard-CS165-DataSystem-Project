/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include "threadpool.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/

char** execute_create(DbOperator* query, ClientContext* context) {
    if(query->operator_fields.create_operator.create_type == _DB){
        if (create_db(query->operator_fields.create_operator.name).code == OK) {
            // return "Successfully created database";
            return "";
        } else {
            return "Failed";
        }
    } else if(query->operator_fields.create_operator.create_type == _TABLE){
        Status create_status;
        create_table(
            query->operator_fields.create_operator.db, 
            query->operator_fields.create_operator.name, 
            query->operator_fields.create_operator.col_count, 
            &create_status
        );
        if (create_status.code != OK) {
            cs165_log(stdout, "adding a table failed.");
            return "Failed";
        }
        // return "Successfully created table";
        return "";
    } else if (query->operator_fields.create_operator.create_type == _COLUMN) {
        Status create_status;
        Column* column = create_column(
            query->operator_fields.create_operator.name,
            query->operator_fields.create_operator.table,
            query->operator_fields.create_operator.db,
            &create_status
        );

        if (column == NULL || create_status.code != OK) {
            cs165_log(stdout, "adding a column failed.");
            return "Failed";
        }

        add_column_pointer(
            context, 
            query->operator_fields.create_operator.db,
            query->operator_fields.create_operator.table,
            column
        );

        // return "Successfully created column";
        return "";
    } else if (query->operator_fields.create_operator.create_type == _INDEX) {
        Status create_status = create_index(
            query->operator_fields.create_operator.table,
            query->operator_fields.create_operator.column,
            query->operator_fields.create_operator.indexType,
            query->operator_fields.create_operator.clusterType
        );
        if (create_status.code != OK) {
            cs165_log(stdout, "adding an index failed.");
            return "Failed";
        }
        // return "Successfully created column";
        return "";
    }
}


char* execute_insert(DbOperator* query) {
    Table* table = query->operator_fields.insert_operator.table;
    int* values = query->operator_fields.insert_operator.values;
    Status insert_status = relational_insert(table, values);
    if (insert_status.code != OK) {
        cs165_log(stdout, "insert a row failed");
        return "Failed";
    }
    // todo: update context when column pointer address changes
    // return "Successfully inserted a row";
    return "";
}


char* execute_select(DbOperator* query, ClientContext* context) {
    SelectResult select_res = {"", NULL, 0};
    Status status;
    if (query->operator_fields.select_operator.select_type == COLUMN_SELECT) {
        status = select_column(
            query->operator_fields.select_operator.select_operator_field.cso.res_name,
            query->operator_fields.select_operator.select_operator_field.cso.table,
            query->operator_fields.select_operator.select_operator_field.cso.column,
            &select_res,
            query->operator_fields.select_operator.select_operator_field.cso.cmp
        );
    } else {
        status = select_fetch(
            query->operator_fields.select_operator.select_operator_field.fso.res_name,
            query->operator_fields.select_operator.select_operator_field.fso.fetch_res,
            query->operator_fields.select_operator.select_operator_field.fso.select_res,
            query->operator_fields.select_operator.select_operator_field.fso.cmp,
            &select_res
        );
    }
    if (status.code != OK) {
        cs165_log(stdout, "select failed");
        return "Failed";
    }
    add_select_result(context, select_res);
    // return "Select the rows successfully";
    return "";
}


void* select_wrapper(void* arg) {
    SelectArgWrapper* unwrapped_struct = (SelectArgWrapper*)arg;
    execute_select(unwrapped_struct->dbo, unwrapped_struct->context);
    free(unwrapped_struct);
    return NULL;
}


char* execute_select_batch(DbOperator* query, ClientContext* context) {
    SelectArgWrapper* wrapper = (SelectArgWrapper*)malloc(sizeof(SelectArgWrapper));
    wrapper->context = context;
    wrapper->dbo = query;
    add_worker(select_wrapper, wrapper, context->pool);
    return "";
}


char* execute_fetch(DbOperator* query, ClientContext* context) {
    FetchResult* fetch_result = malloc(sizeof(FetchResult));
    Status status = fetch_select(
        query->operator_fields.fetch_operator.res_name,
        query->operator_fields.fetch_operator.table,
        query->operator_fields.fetch_operator.column,
        query->operator_fields.fetch_operator.select_result,
        fetch_result
    );
    if (status.code != OK) {
        cs165_log(stdout, "fetch failed");
        return "failed";
    }
    add_fetch_result(context, fetch_result);
    // return "Fetch the select result successfully";
    return "";
}


char* execute_print(DbOperator* query, ClientContext* context) {
    char* result = (char*)malloc(sizeof(char) * MAX_OUTPUT_SIZE);
    memset(result, 0, sizeof(result));
    // char result[MAX_OUTPUT_SIZE] = "";
    int num_prints = query->operator_fields.print_operator.num_name;
    PrintStruct prints[num_prints];
    for (int i = 0; i < num_prints; i++) {
        prints[i] = get_print(
            context, 
            query->operator_fields.print_operator.names[i]
        );
    }
    Status status = print_list(prints, num_prints, result);
    if (status.code != OK) {
        cs165_log(stdout, "print failed");
        return "failed";
    }
    return result;
}


char* execute_load(DbOperator* query, ClientContext* context) {
    Status status = load_file(query->operator_fields.load_operator.path);
    if (status.code != OK) {
        cs165_log(stdout, "load failed");
        return "failed";
    }
    // return "Successfully loaded csv file";
    return "";
}


char* execute_shutdown(DbOperator* query, ClientContext* context) {
    Status status = shutdown_server();
    if (status.code != OK) {
        cs165_log(stdout, "load failed");
        return "failed";
    }
    clean_context(context);
    // return "server shut down and data saved";
    exit(0);
    return "";
}


char* execute_agg(DbOperator* query, ClientContext* context) {
    long double agg_result = 0;

    GeneralizedColumnHandle* handle = get_chandle(
        context, 
        query->operator_fields.agg_operator.handle
    );
 
    Status status = compute_aggregate(
        handle,
        query->operator_fields.agg_operator.type,
        &agg_result
    );

    if (status.code != OK) {
        cs165_log(stdout, "compute aggregate failed");
        return "failed";
    }

    Aggregate agg = {"", query->operator_fields.agg_operator.type, agg_result};
    strcpy(
        agg.name, 
        query->operator_fields.agg_operator.res_name
    );
    add_aggregate(context, agg);

    // return "Compute aggregates successfully";
    return "";
}


char* execute_math(DbOperator* query, ClientContext* context) {
    FetchResult* fetch_result = malloc(sizeof(FetchResult));
    GeneralizedColumnHandle* first = get_chandle(
        context, 
        query->operator_fields.math_operator.handle1
    );
    GeneralizedColumnHandle* second = get_chandle(
        context,
        query->operator_fields.math_operator.handle2
    );
    char* res_name = query->operator_fields.math_operator.res_name;
    MathType type = query->operator_fields.math_operator.type;
    Status status = compute_math(first, second, type, res_name, fetch_result);
    if (status.code != OK) {
        cs165_log(stdout, "fetch failed");
        return "failed";
    }
    add_fetch_result(context, fetch_result);
    // return "Computed the math successfully";
    return "";
}


char* execute_batch_query(DbOperator* query, ClientContext* context) {
    context->batch = 1;
    create_pool(MAX_THREAD_NUM, &(context->pool));
    return "";
}


char* execute_batch_exec(DbOperator* query, ClientContext* context) {
    context->batch = 0;
    while (context->pool->head) { }
    destroy_pool(context->pool);
    return "";
}

char* execute_join(DbOperator* query, ClientContext* context) {
    SelectResult res1;
    SelectResult res2;
    strcpy(res1.select_name, query->operator_fields.join_operator.r1n);
    strcpy(res2.select_name, query->operator_fields.join_operator.r2n);
    join_selects(
        query->operator_fields.join_operator.f1.data,
        query->operator_fields.join_operator.p1.id,
        query->operator_fields.join_operator.f2.data,
        query->operator_fields.join_operator.p2.id,
        query->operator_fields.join_operator.f1.num_rows,
        query->operator_fields.join_operator.f2.num_rows,
        query->operator_fields.join_operator.jtype,
        &res1, &res2
    );
    add_select_result(context, res1);
    add_select_result(context, res2);
    return "";
}

char* execute_DbOperator(DbOperator* query, ClientContext* context) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    if(!query)
    {
        // return "165";
        return "";
    }
    if(query && query->type == CREATE){
        return execute_create(query, context);
    } else if (query && query->type == INSERT) {
        return execute_insert(query);
    } else if (query && query->type == SELECT) {
        if (context->batch == 1) {
            return execute_select_batch(query, context);
        } else {
            return execute_select(query, context);
        }
    } else if (query && query->type == FETCH) {
        return execute_fetch(query, context);
    } else if (query && query->type == PRINT) {
        return execute_print(query, context);
    } else if (query && query->type == LOAD) {
        return execute_load(query, context);
    } else if (query && query->type == SHUTDOWN) {
        return execute_shutdown(query, context);
    } else if (query && query->type == AGG) {
        return execute_agg(query, context);
    } else if (query && query->type == MATH) {
        return execute_math(query, context);
    } else if (query && query->type == BATCH_QUERY) {
        return execute_batch_query(query, context);
    } else if (query && query->type == BATCH_EXEC) {
        return execute_batch_exec(query, context);
    } else if (query && query->type == JOIN) {
        return execute_join(query, context);
    }
    free(query);
    // return "165";
    return "";
}

int floor_func(int num_bytes, int buffer_size) {
    int n = (int)(num_bytes / buffer_size);
    if (num_bytes % buffer_size > 0) {
        n++;
    }
    return n;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = malloc(sizeof(ClientContext));
    
    db_startup();
    initialize_context(client_context);
    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            char* result = execute_DbOperator(query, client_context);

            send_message.length = strlen(result);
            // char send_buffer[send_message.length + 1];
            // strcpy(send_buffer, result);
            send_message.payload = NULL;
            send_message.status = OK_WAIT_FOR_RESPONSE;
            
            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            int num_buffers = floor_func(send_message.length, DEFAULT_QUERY_BUFFER_SIZE);
            for (int i = 0; i < num_buffers; i++) {
                char send_buffer[DEFAULT_QUERY_BUFFER_SIZE + 1] = "";
                int buffer_len = DEFAULT_QUERY_BUFFER_SIZE;
                if (i == num_buffers - 1) {
                    buffer_len = send_message.length - i * DEFAULT_QUERY_BUFFER_SIZE;
                }
                strncpy(send_buffer, result + i * DEFAULT_QUERY_BUFFER_SIZE, buffer_len);
                send_buffer[buffer_len] = '\0';
                if (send(client_socket, send_buffer, buffer_len, 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }
            }
            // free(result);
        }
    } while (!done);
    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
// 
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients? 
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }
    
    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;
    while (1) {
        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }
        handle_client(client_socket);
    }
    return 0;
}
