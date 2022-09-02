#ifndef TABLE_H
#define TABLE_H
#include "cs165_api.h"

int double_row_cap(Table* table);
int copy_table(Table* dst, Table* src, int len);
int persist_table(Table* table);
int restore_table(Table* table);
int double_table_list(Db* db);


#endif