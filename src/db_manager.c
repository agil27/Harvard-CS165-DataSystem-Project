#include "cs165_api.h"
#include <string.h>
#include "column.h"
#include "io.h"
#include "table.h"
#include "client_context.h"
#include "column.h"
#include "client_context.h"
#include "utils.h"
#include "btree.h"
#include "join.h"

// In this class, there will always be only one active database at a time
Db *current_db;


Status relational_insert(Table* table, int* val) {
	// adjust the max capacity for the number of rows
	Status ret_status;
	ret_status.code = OK;
	if ((table->table_length) == (table->row_cap)) {
		if (double_row_cap(table)) {
			ret_status.code = ERROR;
			return ret_status;
		}
	}
	// insert a new row
	table->table_length++;
	for (int i = 0; i < table->col_count; i++) {
		Column* column = &table->columns[i];
		if (column->data == NULL) {
			if (lazy_load(table, column)) {
				ret_status.code = ERROR;
				return ret_status;
			}
		}
		column->data[table->table_length - 1] = val[i];
	}
	return ret_status;
}


Status create_index(Table* table, Column* column, IndexType indexType, ClusterType ClusterType) {
	Status ret_status;
	ret_status.code = OK;
	column->index_type = indexType;
	if (ClusterType == CLUSTERED) {
		column->clustered = true;
		for (int i = 0; i < table->col_num; i++) {
			if (&table->columns[i] == column) {
				table->cluster_index = i;
				break;
			}
		}
	} else {
		column->clustered = false;
	}
	return ret_status;
}

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	// to do: judge repeating names

	// adjust the database capacity
	if ((db->tables_size) == (db->tables_capacity)) {
		if (double_table_list(db)) {
			ret_status->code = ERROR;
			return NULL;
		}
	}

	// insert a new table to the list
	Table* table = &((db->tables)[(db->tables_size)++]);
	// todo: strcpy failure
	strcpy(table->name, name);
	table->col_count = num_columns;
	table->col_num = 0;
	table->table_length = 0;
	table->row_cap = INIT_TABLE_ROW_CAP;
	table->cluster_index = -1;

	// allocate memory space for the fixed length columns
	if ((table->columns = (Column*)malloc(sizeof(Column) * num_columns)) == NULL) {
		ret_status->code = ERROR;
		return NULL;
	}
	ret_status->code = OK;
	log_info("create table [%s] success!", name);
	return table;
}

/* 
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char* db_name) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	struct Status ret_status;
	if ((current_db = (Db*)(malloc(sizeof(Db)))) == NULL) {
		ret_status.code = ERROR;
		return ret_status;
	}
	// todo: strcpy failure
	strcpy(current_db->name, db_name);
	current_db->tables = (Table*)malloc(sizeof(Table));
	current_db->tables_capacity = INIT_TABLE_CAPACITY;
	current_db->tables_size = 0;
	ret_status.code = OK;
	log_info("create db [%s] success\n", db_name);
	return ret_status;
}


/*
* This function is meant to create a column for a table
*/
Column* create_column(const char* col_name, Table* table, Db* db, Status* status) {
	Status ret_status;
	if (table == NULL) {
		ret_status.code = ERROR;
		return NULL;
	}
	Column* column = create_column_from_table(table, col_name);
	if (column == NULL) {
		ret_status.code = ERROR;
	} else {
		ret_status.code = OK;
	}
	*status = ret_status;
	return column;
}


/*
* this function persists database metadata & table metadata & column data
*/
int persist_db() {
	FILE* f;
	if ((f = fopen(DB_METADATA_PATH, "w")) == NULL) {
		return -1;
	}
	fwrite(current_db, sizeof(Db), 1, f);
	fwrite(current_db->tables, sizeof(Table), current_db->tables_size, f);
	fclose(f);
	for (int i = 0; i < current_db->tables_size; i++) {
		if (persist_table(&((current_db->tables)[i]))) {
			return -1;
		}
	}
	free(current_db->tables);
	free(current_db);
	current_db = NULL;
	return 0;
}


int restore_db() {
	FILE* f;
	if ((f = fopen(DB_METADATA_PATH, "r")) == NULL) {
		current_db = 0;
		return 0;
	}
	current_db = (Db*)(malloc(sizeof(Db)));
	fread(current_db, sizeof(Db), 1, f);
	if ((current_db->tables = (Table*)malloc(current_db->tables_capacity * sizeof(Table))) == NULL) {
		return -1;
	}
	fread(current_db->tables, sizeof(Table), current_db->tables_size, f);
	fclose(f);
	for (int i = 0; i < current_db->tables_size; i++) {
		if (restore_table(&((current_db->tables)[i]))) {
			return -1;
		}
	}
	return 0;
}


/*
* this function shut down the server and persists data & metadata
*/
Status shutdown_server() {
	/* save metadata */
	rmrf("data");
	safe_mkdir("data");
	Status ret_status;
	ret_status.code = OK;
	if (persist_db()) {
		ret_status.code = ERROR;
	}
	return ret_status;
}


Status db_startup() {
	Status ret_status;
	ret_status.code = OK;
	if (restore_db()) {
		ret_status.code = ERROR;
	}
	return ret_status;
}


Status select_column(const char* res_name, Table* table, Column* column, SelectResult* res, Comparator cmp) {
	Status ret_status;
	ret_status.code = OK;
	int* viable_id = (int*)malloc((table->table_length) * sizeof(int));
	int num_ids = 0;
	if (column->data == NULL) {
		if (lazy_load(table, column)) {
			ret_status.code = ERROR;
			return ret_status;
		}
	}
	// for (int i = 0 ; i < table->table_length; i++) {
	// 	if (satisfy(column->data[i], cmp)) {
	// 		viable_id[num_ids++] = i;
	// 	}
	// }
	if (column->index_type == NONE) {
		for (int i = 0 ; i < table->table_length; i++) {
			if (satisfy(column->data[i], cmp)) {
				viable_id[num_ids++] = i;
			}
		}
	} 
	// else {
	// 	num_ids = binary_search_range(column->index.sorted, column->data, viable_id, cmp.p_low, cmp.p_high, table->table_length);
	// 	printf("numid %d\n", num_ids);
	// }

	else if (column->index_type == BTREE) {
		num_ids = btree_search_range(column->index.btree, cmp.p_low, cmp.p_high, viable_id);
	} 
	else {
		num_ids = binary_search_range(column->index.sorted, column->data, viable_id, cmp.p_low, cmp.p_high, table->table_length);
	}
	
	strcpy(res->select_name, res_name);
	res->id = viable_id;
	res->num_ids = num_ids;
	return ret_status;
}


Status select_fetch(const char* res_name, FetchResult* fetch_result, SelectResult select_result, Comparator cmp, SelectResult* res) {
	Status ret_status;
	ret_status.code = OK;
	int* viable_id = (int*)malloc(sizeof(int) * fetch_result->num_rows);
	int* candidate_id = select_result.id;
	int num_ids = 0;

	for (int i = 0 ; i < fetch_result->num_rows; i++) {
		if (satisfy(fetch_result->data[i], cmp)) {
			viable_id[num_ids++] = candidate_id[i];
		}
	}
	strcpy(res->select_name, res_name);
	res->id = viable_id;
	res->num_ids = num_ids;
	return ret_status;
}


Status fetch_select(const char* res_name, Table* table, Column* column, SelectResult select_res, FetchResult* res) {
	Status ret_status;
	ret_status.code = OK;
	if (column->data == NULL) {
		if (lazy_load(table, column)) {
			ret_status.code = ERROR;
			return ret_status;
		}
	}
	strcpy(res->fetch_name, res_name);
	res->num_rows = select_res.num_ids;
	res->data = (int*)(malloc(select_res.num_ids * sizeof(int)));
	// if select_res exists, the column data must have been read into the memory, so no need to read it again
	for (int i = 0; i < select_res.num_ids; i++) {
		(res->data)[i]= column->data[select_res.id[i]];
	}
	return ret_status;
}


// todo: remove the memory copy here
int fetch_column(Table* table, Column* column, FetchResult* res) {
	if (column->data == NULL) {
		if (lazy_load(table, column)) {
			return -1;
		}
	}

	// todo: assign name on the client query
	strcpy(res->fetch_name, table->name);
	strcat(res->fetch_name, ".");
	strcat(res->fetch_name, column->name);

	res->num_rows = table->table_length;
	res->data = (int*)(malloc(table->table_length * sizeof(int)));
	memcopy(res->data, column->data, table->table_length);
	return 0;
}


int print_column(int* data, int len, char* result) {
	for (int i = 0; i < len; i++) {
		char num_str[MAX_INT_SIZE] = "";
		sprintf(num_str, "%d\n", data[i]);
		strcat(result, num_str);
	}
	return 0;
}


Status print_list(PrintStruct* prints, int num_prints, char* result) {
	Status ret_status;
	ret_status.code = OK;
	for (int i = 0; i < num_prints; i++) {
		// char* column_result = (char*)malloc(sizeof(char) * MAX_SINGLE_OUTPUT_SIZE);
		char column_result[MAX_SINGLE_OUTPUT_SIZE] = "";
		if (prints[i].type == CHANDLE) {
			int* column_data = NULL;
			int num_rows = 0;
			if (prints[i].value.chandle->generalized_column.column_type == COLUMN) {
				column_data = prints[i].value.chandle->generalized_column.column_pointer.column.column_data->data;
				if (column_data == NULL) {
					Table* table = prints[i].value.chandle->generalized_column.column_pointer.column.table;
					Column* column = prints[i].value.chandle->generalized_column.column_pointer.column.column_data;
					if (lazy_load(table, column)) {
						ret_status.code = ERROR;
						return ret_status;
					}
				}
				num_rows = prints[i].value.chandle->generalized_column.column_pointer.column.table->table_length;
			} else {
				// result
				column_data = prints[i].value.chandle->generalized_column.column_pointer.result->data;
				num_rows = prints[i].value.chandle->generalized_column.column_pointer.result->num_rows;
			}
			if (print_column(column_data, num_rows, column_result)) {
				ret_status.code = ERROR;
				return ret_status;
			}
		} else {
			if (prints[i].value.aggregate->type == AVG) {
				if (i < num_prints - 1) {
					sprintf(column_result, "%.2Lf, ", prints[i].value.aggregate->value);
				} else {
					sprintf(column_result, "%.2Lf\n", prints[i].value.aggregate->value);
				}
			} else {
				if (i < num_prints - 1) {
					sprintf(column_result, "%ld, ", (long)prints[i].value.aggregate->value);
				} else {
					sprintf(column_result, "%ld\n", (long)prints[i].value.aggregate->value);
				}
			}
		}
		strcat(result, column_result);
		// free(column_result);
	}
	return ret_status;
}


Status compute_aggregate(GeneralizedColumnHandle* handle, AggType type, long double* res) {
	Status status;
	if (handle == NULL) {
		status.code = ERROR;
		return status;
	}

	int* data = get_chandle_data(handle);
	int n = get_chandle_len(handle);

	if (type == SUM) {
		long sum = 0;
		for (int i = 0; i < n; i++) {
			sum += (long)data[i];	
			// printf("%ld %d\n", sum, data[i]);
		}
		*res = (long double)sum;
	} else if (type == AVG) {
		if (n == 0) {
			*res = 0;
		} else {
			long double avg = 0;
			for (int i = 0; i < n; i++) {
				avg += (long double)data[i];
			}
			avg /= (long double)n;
			*res = avg;
		}
	} else if (type == MAX) {
		long double max = data[0];
		for (int i = 0; i < n; i++) {
			if (data[i] > max) {
				max = data[i];
			}
		}
		*res = max;
	} else if (type == MIN) {
		double min = data[0];
		for (int i = 0; i < n; i++) {
			if (data[i] < min) {
				min = data[i];
			}
		}
		*res = min;
	}
	status.code = OK;
	return status;
}


Status compute_math(GeneralizedColumnHandle* first, GeneralizedColumnHandle* second, MathType type, const char* res_name, FetchResult* fetch_result) {
	Status status;
	int* val1 = get_chandle_data(first);
	int len1 = get_chandle_len(first);
	int* val2 = get_chandle_data(second);
	int len2 = get_chandle_len(second);
	if (len1 != len2) {
		status.code = ERROR;
		return status;
	}
	fetch_result->num_rows = len1;
	strcpy(fetch_result->fetch_name, res_name);
	fetch_result->data = (int*)malloc(sizeof(int) * len1);
	if (type == ADD) {
		for (int i = 0; i < len1; i++) {
			fetch_result->data[i] = val1[i] + val2[i];
		}
	} else {
		for (int i = 0; i < len1; i++) {
			fetch_result->data[i] = val1[i] - val2[i];
		}
	}
	status.code = OK;
	return status;
}


int setup_index(Column* column, int num_rows) {
	if (column->index_type != NONE) {
		int* key = NULL;
		int* val = (int*)malloc(sizeof(int) * num_rows);
		for (int i = 0; i < num_rows; i++) {
			val[i] = i;
		}
		column->index.sorted = val;
		if (!column->clustered) {
			key = (int*)malloc(sizeof(int) * num_rows);
			memcopy(key, column->data, num_rows);
			quicksort(key, val, 0, num_rows - 1);
		} else {
			key = column->data;
		}
		if (column->index_type == BTREE) {
			column->index.btree = build_btree(key, val, num_rows);
		}
	}
}


Status load_file(const char* path) {
	Status status;
	int num_rows = 0;
    int num_cols = 0;
    char* col_names[MAX_COL];
    char table_name[MAX_SIZE_NAME] = "";
    int** cols = load_csv(path, table_name, col_names, &num_cols, &num_rows);
    
	if (cols == NULL) {
		status.code = ERROR;
		return status;
	}

	printf("table name: %s\n", table_name);

	int num_tables = current_db->tables_size;
	Table* db_table;
	for (int i = 0; i < num_tables; i++) {
		if (strcmp(current_db->tables[i].name, table_name) == 0) {
			db_table = &(current_db->tables[i]);
		}
	}
	
	db_table->table_length = num_rows;
	// todo: make sure the table capacity > num_rows
	if (db_table->row_cap < num_rows) {
		db_table->row_cap = num_rows + 1;
	}

	printf("col names: ");
	for (int i = 0; i < num_cols; i++) {
		printf("%s ", col_names[i]);
	}
	printf("\n");

	int* index = (int*)malloc(sizeof(int) * num_rows);
	for (int i = 0; i < num_rows; i++) {
		index[i] = i;
	}
	if (db_table->cluster_index != -1) {
		quicksort(cols[db_table->cluster_index], index, 0, num_rows - 1);
	}

    for (int i = 0; i < num_cols; i++) {
		if (db_table->row_cap < num_rows) {
			free(db_table->columns[i].data);
			db_table->columns[i].data = (int*)malloc(sizeof(int) * (num_rows + 1));
		}
        strcpy(db_table->columns[i].name, col_names[i]);
		// memcopy(db_table->columns[i].data, cols[i], num_rows);
		if (i != db_table->cluster_index || db_table->cluster_index == -1) {
			for (int j = 0; j < num_rows; j++) {
				db_table->columns[i].data[j] = cols[i][index[j]];
			}
		} else {
			for (int j = 0; j < num_rows; j++) {
				db_table->columns[i].data[j] = cols[i][j];
			}
		}
		free(cols[i]);
		// todo: load by names
		// todo: simplify the load by directly write to columns instead of a memcopy
    }
	free(index);
	for (int i = 0; i < db_table->col_num; i++) {
		setup_index(&db_table->columns[i], db_table->table_length);
	}
	if (db_table->row_cap < num_rows) {
		db_table->row_cap = num_rows + 1;
	}
	free(cols);
	status.code = OK;
	return status;
}


join_selects(int* f1, int* p1, int* f2, int* p2, int n1, int n2, JoinType type, SelectResult* res1, SelectResult* res2) {
	int n = (n1 > n2) ? n1 : n2;
	int* r1 = (int*)malloc(sizeof(int) * n1 * n2);
	int* r2 = (int*)malloc(sizeof(int) * n1 * n2);
	int num_rows = 0;
	int s1 = 0;
	int s2 = 0;
	if (type == HASH) {
		hash_join(p1, f1, p2, f2, n1, n2, r1, r2, &s1, &s2);
	} else {
		nested_looped_join(p1, f1, p2, f2, n1, n2, r1, r2, &s1, &s2);
	}
	int* copy1 = (int*)malloc(sizeof(int) * s1);
	int* copy2 = (int*)malloc(sizeof(int) * s2);
	// int* s1 = r1;
	// int* s2 = r2;
	memcopy(copy1, r1, s1);
	memcopy(copy2, r2, s2);
	free(r1);
	free(r2);
	res1->num_ids = s1;
	res2->num_ids = s2;
	res1->id = copy1;
	res2->id = copy2;
}

