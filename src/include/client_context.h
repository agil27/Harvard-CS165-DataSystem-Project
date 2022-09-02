#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);
int lookup_column_table(char* name, Column** column, Table** table);
int initialize_context(ClientContext* context);
int add_select_result(ClientContext* context, SelectResult result);
int add_fetch_result(ClientContext* context, FetchResult* fetch_res);
SelectResult get_select_res(ClientContext* context, const char* name);
GeneralizedColumnHandle* get_chandle(ClientContext* context, const char* name);
Aggregate* get_aggregate(ClientContext* context, const char* name);
PrintStruct get_print(ClientContext* context, const char* name);
int add_column_pointer(ClientContext* context, Db* db, Table* table, Column* column);
int add_handle(ClientContext* context, GeneralizedColumnHandle handle);
int add_aggregate(ClientContext* context, Aggregate agg);
int clean_chandles(ClientContext* context);
int clean_select_results(ClientContext* context);
int clean_aggregates(ClientContext* context);
int clean_select_results(ClientContext* context);
int get_chandle_len(GeneralizedColumnHandle* handle);
int* get_chandle_data(GeneralizedColumnHandle* handle);
#endif
