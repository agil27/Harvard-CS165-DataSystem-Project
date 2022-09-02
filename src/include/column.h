#ifndef COLUMN_H
#define COLUMN_H
#include "cs165_api.h"
#include "io.h"

int persist_column(Column* column, const char* path, int num_rows);
int restore_column(Column* column, const char* path, int num_rows);
int flush_column(Column* column, const char* table_path, int num_rows);
int flush_index(Column* column, const char* table_path, int num_rows);
int load_column(Column* column, const char* table_path, int num_rows);
int lazy_load(Table* table, Column* column);
int load_index_from_table(Table* table, Column* column);

Column* create_column_from_table(Table* table, const char* col_name);

#endif