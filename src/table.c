#include "table.h"
#include "cs165_api.h"
#include "io.h"
#include "column.h"


int double_row_cap(Table* table) {
	table->row_cap *= 2;
	for (int i = 0; i < table->col_count; i++) {
		int* new_data;
		if ((new_data = (int*)(malloc(table->row_cap * sizeof(int)))) == NULL) {
			return -1;
		}
		if (memcopy(new_data, table->columns[i].data, table->table_length)) {
			return -1;
		}
		free(table->columns[i].data);
		table->columns[i].data = new_data;
	}
	return 0;
}


int copy_table(Table* dst, Table* src, int len) {
	Table* dst_ptr = dst;
	Table* src_ptr = src;
	for (int i = 0; i < len; i++) {
		if (src_ptr) {
			*(dst_ptr++) = *(src_ptr++);
		} else {
			return -1;
		}
	}
	return 0;
}


int persist_table(Table* table) {
	char table_path[MAX_SIZE_NAME + 10] = TABLE_BASE_PATH;
	if (safe_mkdir(table_path)) {
		return -1;
	}
	strcat(table_path, table->name);
	if (safe_mkdir(table_path)) {
		return -1;
	}

	char table_metapath[MAX_SIZE_NAME + 15] = "";
	strcpy(table_metapath, table_path);
	strcat(table_metapath, "metadata");
	FILE* f;
	if ((f = fopen(table_metapath, "w")) == NULL) {
		return -1;
	}
	fwrite(table->columns, sizeof(Column), table->col_num, f);
	fclose(f);

	for (int i = 0; i < table->col_num; i++) {
		Column* column = &(table->columns[i]);
		// only flush the loaded columns
		if (column->data) {
			if (flush_column(column, table_path, table->table_length)) {
				return -1;
			}
		}
		flush_index(column, table_path, table->table_length);
	}
	free(table->columns);
	table->columns = NULL;
	return 0;
}


/*
* this function restore table metadata and recreate the table
*/
int restore_table(Table* table) {
	char table_path[MAX_SIZE_NAME + 10] = TABLE_BASE_PATH;
	strcat(table_path, table->name);
	
	char table_metapath[MAX_SIZE_NAME + 15] = "";
	strcpy(table_metapath, table_path);
	strcat(table_metapath, "metadata");
	FILE* f;
	if ((f = fopen(table_metapath, "r")) == NULL) {
		return -1;
	}
	if ((table->columns = (Column*)malloc(table->col_num * sizeof(Column))) == NULL) {
		return -1;
	}
	fread(table->columns, sizeof(Column), table->col_num, f);
	fclose(f);

	for (int i = 0; i < table->col_num; i++) {
		Column* column = &(table->columns[i]);
		// lazy load
		column->data = NULL;
		column->index.sorted = NULL;
		lazy_load(table, column);
		if (column->index_type != NONE) {
			load_index_from_table(table, column);
			if (column->index_type == BTREE) {
				int* val = (int*)malloc(sizeof(int) * table->table_length);
				for (int i = 0; i < table->table_length; i++) {
					val[i] = column->data[column->index.sorted[i]];
				}
				column->index.btree = build_btree(val, column->index.sorted, table->table_length);
			}
		}
	}
	return 0;
}


int double_table_list(Db* db) {
	db->tables_capacity *= 2;
	Table* new_tables = NULL;
	if ((new_tables = (Table*)(malloc(sizeof(Table) * (db->tables_capacity)))) == NULL) {
		return -1;
	}
	if (copy_table(new_tables, db->tables, db->tables_size)) {
		return -1;
	}
	free(db->tables);
	db->tables = new_tables;
	return 0;
}