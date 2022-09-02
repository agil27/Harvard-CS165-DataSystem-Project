#include "column.h"

/*
* this function writes a column to a given path
*/
int persist_column(Column* column, const char* path, int num_rows) {
	int f = open(path, O_RDWR | O_CREAT, (mode_t)0600);
	if (num_rows == 0) {
		close(f);
		return 0;
	}
	int* mapped;
	if ((mapped = map_file(f, num_rows)) == NULL) {
		return -1;
	}
	if (memcopy(mapped, column->data, num_rows)) {
		return -1;
	}
	if (unmap_file(mapped, f, num_rows)) {
		return -1;
	}
	free(column->data);
	column->data = NULL;
	return 0;
}


int persist_index(Column* column, const char* path, int num_rows) {
	int f = open(path, O_RDWR | O_CREAT, (mode_t)0600);
	if (num_rows == 0) {
		close(f);
		return 0;
	}
	int* mapped;
	if ((mapped = map_file(f, num_rows)) == NULL) {
		return -1;
	}
	if (memcopy(mapped, column->index.sorted, num_rows)) {
		return -1;
	}
	if (unmap_file(mapped, f, num_rows)) {
		return -1;
	}
	free(column->index.sorted);
	column->index.sorted = NULL;
	// column->index.btree = NULL;
	return 0;
}


/*
* this function restore a column from the binary file
*/
int restore_column(Column* column, const char* path, int num_rows) {
	int f = open(path, O_RDWR | O_CREAT, (mode_t)0600);
	int* mapped;

	if ((mapped = map_file(f, num_rows)) == NULL) {
		return 0;
	}

	if ((column->data = (int*) malloc(num_rows * sizeof(int))) == NULL) {
		return -1;
	}

	if (memcopy(column->data, mapped, num_rows)) {
		return -1;
	}

	if (unmap_file(mapped, f, num_rows)) {
		return -1;
	}

	return 0;
}


int restore_index(Column* column, const char* path, int num_rows) {
	int f = open(path, O_RDWR | O_CREAT, (mode_t)0600);
	int* mapped;
	if ((mapped = map_file(f, num_rows)) == NULL) {
		return 0;
	}
	if ((column->index.sorted = (int*) malloc(num_rows * sizeof(int))) == NULL) {
		return -1;
	}
	if (memcopy(column->index.sorted, mapped, num_rows)) {
		return -1;
	}
	if (unmap_file(mapped, f, num_rows)) {
		return -1;
	}
	return 0;
}


/*
* this function persists table metadata and its column data
*/

int flush_column(Column* column, const char* table_path, int num_rows) {
	char column_path[MAX_SIZE_NAME * 2 + 10] = "";
	strcpy(column_path, table_path);
	strcat(column_path, "/");
	strcat(column_path, column->name);
	if (column->data) {
		// make sure the data has been lazy-loaded
		if (persist_column(column, column_path, num_rows)) {
			return -1;
		}
	}
	return 0;
}

int flush_index(Column* column, const char* table_path, int num_rows) {
	if (column->index_type != NONE) {
		char column_path[MAX_SIZE_NAME * 2 + 10] = "";
		strcpy(column_path, table_path);
		strcat(column_path, "/");
		strcat(column_path, column->name);
		strcat(column_path, ".index");
		if (persist_index(column, column_path, num_rows)) {
			return -1;
		}
	}
	return 0;
}

int load_column(Column* column, const char* table_path, int num_rows) {
	char column_path[MAX_SIZE_NAME * 2 + 10] = "";
	strcpy(column_path, table_path);
	strcat(column_path, "/");
	strcat(column_path, column->name);
	if (restore_column(column, column_path, num_rows)) {
		return -1;
	}
	return 0;
}


int load_index(Column* column, const char* table_path, int num_rows) {
	char column_path[MAX_SIZE_NAME * 2 + 10] = "";
	strcpy(column_path, table_path);
	strcat(column_path, "/");
	strcat(column_path, column->name);
	strcat(column_path, ".index");
	if (restore_index(column, column_path, num_rows)) {
		return -1;
	}
	return 0;
}


int lazy_load(Table* table, Column* column) {
	char table_path[MAX_SIZE_NAME + 10] = TABLE_BASE_PATH;
	strcat(table_path, table->name);
	if (load_column(column, table_path, table->table_length)) {
		return -1;
	}
	return 0;
}


int load_index_from_table(Table* table, Column* column) {
	char table_path[MAX_SIZE_NAME + 10] = TABLE_BASE_PATH;
	strcat(table_path, table->name);
	if (load_index(column, table_path, table->table_length)) {
		return -1;
	}
	return 0;
}


Column* create_column_from_table(Table* table, const char* col_name) {
	if (table->col_num < table->col_count) {
		Column* current_col = (Column*)(&(table->columns[(table->col_num)++]));
		strcpy(current_col->name, col_name);
		current_col->data = (int*)malloc(sizeof(int) * table->row_cap);
		current_col->index.btree = NULL;
		current_col->index.sorted = NULL;
		current_col->index_type = NONE;
		current_col->clustered = 0;
		return current_col;
	}
	return NULL;
}