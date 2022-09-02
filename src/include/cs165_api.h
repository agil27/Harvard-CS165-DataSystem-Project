

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H
#define _GNU_SOURCE
#define _XOPEN_SOURCE 500
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include<sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <ftw.h>
#include <string.h>
#include <limits.h>
#include "threadpool.h"
#include "btree.h"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define INIT_TABLE_ROW_CAP 2000000
#define INIT_TABLE_CAPACITY 16
#define MAX_RESULT_SIZE 512
#define MAX_MESSAGE_LEN 256
#define MAX_PRINT_NUM 16
#define MAX_INT_SIZE 10
#define BASE_PATH "data"
#define TABLE_BASE_PATH "data/table/"
#define DB_METADATA_PATH "data/db.data"
#define MAX_OUTPUT_SIZE 2000000
#define MAX_SINGLE_OUTPUT_SIZE 2000000
#define MAX_COL 32
#define MAX_PATH_SIZE 128
#define MAX_THREAD_NUM 1

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT
} DataType;

struct Comparator;

typedef struct ColumnIndex {
    int* sorted;
    BTree btree;
} ColumnIndex;

typedef enum IndexType {
    BTREE,
    SORTED,
    NONE
} IndexType;

typedef enum ClusterType {
    CLUSTERED,
    UNCLUSTERED
} ClusterType;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;
    ColumnIndex index;
    bool clustered;
    IndexType index_type;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count; // expected column count in total
    size_t col_num; // current column numbers
    size_t table_length; // number of rows
    size_t row_cap; // max number of rows
    int cluster_index;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    int *payload;
} Result;

typedef struct FetchResult {
    char fetch_name[MAX_SIZE_NAME];
    int* data;
    int num_rows;
} FetchResult;

typedef struct ColumnPointer {
    Column* column_data;
    Table* table;
} ColumnPointer;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    FetchResult* result;
    ColumnPointer column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

typedef enum AggType {
    AVG,
    MAX,
    MIN,
    SUM
} AggType;

typedef struct Aggregate {
    char name[MAX_SIZE_NAME];
    AggType type;
    long double value;
} Aggregate;

typedef union PrintValue{
    GeneralizedColumnHandle* chandle;
    Aggregate* aggregate;
} PrintValue;


typedef enum PrintType {
    UNKNOWN,
    CHANDLE,
    AGGREGATE
} PrintType;


typedef struct PrintStruct {
    PrintType type;
    PrintValue value;
} PrintStruct;


typedef struct SelectResult {
    char select_name[MAX_SIZE_NAME];
    int* id;
    int num_ids;
} SelectResult;

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */


typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
    SelectResult* select_results;
    int select_num;
    int select_slots;
    int agg_num;
    int agg_slots;
    Aggregate* aggregates;
    int batch;
    ThreadPool* pool;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    LOAD,
    SELECT,
    FETCH,
    PRINT,
    AGG,
    SHUTDOWN,
    MATH,
    BATCH_QUERY,
    BATCH_EXEC,
    JOIN
} OperatorType;

typedef enum JoinType {
    HASH,
    NESTED_LOOP
} JoinType;

typedef struct JoinOperator {
    FetchResult f1;
    FetchResult f2;
    SelectResult p1;
    SelectResult p2;
    JoinType jtype;
    char r1n[MAX_SIZE_NAME];
    char r2n[MAX_SIZE_NAME];
} JoinOperator;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
    _INDEX
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */

typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    Column* column;
    IndexType indexType;
    ClusterType clusterType;
    int col_count;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for insertion
 */

typedef struct ColumnSelectOperator {
    Table* table;
    Column* column;
    char res_name[MAX_SIZE_NAME];
    Comparator cmp;
} ColumnSelectOperator;

typedef struct FetchSelectOperator {
    SelectResult select_res;
    FetchResult* fetch_res;
    char res_name[MAX_SIZE_NAME];
    Comparator cmp;
} FetchSelectOperator;

typedef union SelectOperatorField {
    ColumnSelectOperator cso;
    FetchSelectOperator fso;
} SelectOperatorField;

typedef enum SelectType {
    COLUMN_SELECT,
    FETCH_SELECT
} SelectType;

typedef struct SelectOperator {
    SelectType select_type;
    SelectOperatorField select_operator_field;    
} SelectOperator;

typedef struct FetchOperator {
    Table* table;
    Column* column;
    char res_name[MAX_SIZE_NAME];
    SelectResult select_result;
} FetchOperator;

typedef struct PrintOperator {
    int num_name;
    char names[MAX_SIZE_NAME][MAX_PRINT_NUM];
} PrintOperator;

typedef struct LoadOperator {
    char path[MAX_PATH_SIZE];
} LoadOperator;

typedef struct AggOperator {
    AggType type;
    char handle[MAX_SIZE_NAME];
    char res_name[MAX_SIZE_NAME];
} AggOperator;

typedef enum MathType {
    ADD,
    SUB
} MathType;

typedef struct MathOperator {
    MathType type;
    char handle1[MAX_SIZE_NAME];
    char handle2[MAX_SIZE_NAME];
    char res_name[MAX_SIZE_NAME];
} MathOperator;

typedef struct ShutdownOperator {
    int save;
} ShutdownOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AggOperator agg_operator;
    ShutdownOperator shutdown_operator;
    MathOperator math_operator;
    JoinOperator join_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;


typedef struct SelectArgWrapper {
    DbOperator* dbo;
    ClientContext* context;
} SelectArgWrapper;


extern Db *current_db;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
// Status db_startup();

Status create_db(const char* db_name);
Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);
// Column* create_column(Table *table, char *name, bool sorted, Status *ret_status);
Column* create_column(const char* col_name, Table* table, Db* db, Status* status);
Status relational_insert(Table* table, int* val);
Status select_column(const char* res_name, Table* table, Column* column, SelectResult* res, Comparator cmp);
Status fetch_select(const char* res_name, Table* table, Column* column, SelectResult select_res, FetchResult* res);
Status print_list(PrintStruct* prints, int num_prints, char* result);
Status load_file(const char* path);
Status shutdown_server();
Status db_startup();
Status compute_aggregate(GeneralizedColumnHandle* handle, AggType type, long double* res);
Status compute_math(GeneralizedColumnHandle* first, GeneralizedColumnHandle* second, MathType type, const char* res_name, FetchResult* fetch_result);
Status select_fetch(const char* res_name, FetchResult* fetch_result, SelectResult select_result, Comparator cmp, SelectResult* res);
Status create_index(Table* table, Column* column, IndexType indexType, ClusterType ClusterType);
join_selects(int* f1, int* p1, int* f2, int* p2, int n1, int n2, JoinType type, SelectResult* res1, SelectResult* res2);
// char** execute_db_operator(DbOperator* query);
// void db_operator_free(DbOperator* query);


#endif /* CS165_H */



