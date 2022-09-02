#include "client_context.h"
#define INIT_SLOT 128
#include "io.h"
#include "column.h"


Table* lookup_table(char *name) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	char** fullname_list = &name;
    char* db_name = strsep(fullname_list, ".");
    char* tbl_name = *fullname_list;
	Table* table = NULL;
	int db_size = current_db->tables_size;
	for (int i = 0; i < db_size; i++) {
		if (strcmp(current_db->tables[i].name, tbl_name) == 0) {
			table = &current_db->tables[i];
			break;
		}
	}
	return table;
}

int lookup_column_table(char* name, Column** column, Table** table) {
	char** fullname_list = &name;
    char* db_name = strsep(fullname_list, ".");
    char* tbl_name = strsep(fullname_list, ".");
	char* col_name = *fullname_list;

	int db_size = current_db->tables_size;
	for (int i = 0; i < db_size; i++) {
		if (strcmp(current_db->tables[i].name, tbl_name) == 0) {
			*table = &current_db->tables[i];
			break;
		}
	}
	if ((*table) != NULL) {
		int tbl_size = (*table)->col_num;
		for (int j = 0; j < tbl_size; j++) {
			if (strcmp((*table)->columns[j].name, col_name) == 0) {
				*column = &((*table)->columns[j]);
				break;
			}
		}
	}
	return 0;
}


int add_all_loaded_columns(ClientContext* context, Db* db) {
	if (db == NULL) {
		return -1;
	}
	int num_tables = db->tables_size;
	for (int i = 0; i < num_tables; i++) {
		Table* table = &db->tables[i];
		if (table == NULL) {
			return -1;
		}
		int num_cols = table->col_num;
		for (int j = 0; j < num_cols; j++) {
			Column* column = &table->columns[j];
			add_column_pointer(context, db, table, column);
		}
	}
	return 0;
}


int initialize_context(ClientContext* context) {
	if (context == NULL) {
		return -1;
	}
	context->chandle_table = malloc(sizeof(GeneralizedColumnHandle) * INIT_SLOT);
	context->chandle_slots = INIT_SLOT;
	context->chandles_in_use = 0;
	add_all_loaded_columns(context, current_db);
	context->select_results = (SelectResult*)malloc(sizeof(SelectResult) * INIT_SLOT);
	context->select_slots = INIT_SLOT;
	context->select_num = 0;
	context->agg_num = 0;
	context->agg_slots = INIT_SLOT;
	context->aggregates = (Aggregate*)malloc(sizeof(Aggregate) * INIT_SLOT);
	context->batch = 0;
	context->pool = NULL;
	return 0;
}


int add_select_result(ClientContext* context, SelectResult result) {
	if (context == NULL) {
		return -1;
	}
	if (context->select_num == context->select_slots) {
		context->select_slots *= 2;
		SelectResult* old_results = context->select_results;
		context->select_results = (SelectResult*)malloc(sizeof(SelectResult) * context->select_slots);
		if (context->select_results == NULL) {
			return -1;
		}
		for (int i = 0; i < context->select_slots / 2; i++) {
			context->select_results[i] = old_results[i];
		}
		free(old_results);
	}
	context->select_results[(context->select_num)++] = result;
	return 0;
}


int add_handle(ClientContext* context, GeneralizedColumnHandle handle) {
	if (context == NULL) {
		return -1;
	}
	if (context->chandles_in_use == context->chandle_slots) {
		context->chandle_slots *= 2;
		GeneralizedColumnHandle* old_results = context->chandle_table;
		context->chandle_table = (GeneralizedColumnHandle*)malloc(sizeof(GeneralizedColumnHandle) * context->select_slots);
		if (context->chandle_table == NULL) {
			return -1;
		}
		for (int i = 0; i < context->chandle_slots / 2; i++) {
			context->chandle_table[i] = old_results[i];
		}
		free(old_results);
	}
	context->chandle_table[context->chandles_in_use++] = handle;
	return 0;
}


int add_aggregate(ClientContext* context, Aggregate agg) {
	if (context == NULL) {
		return -1;
	}
	if (context->agg_num == context->agg_slots) {
		context->agg_slots *= 2;
		Aggregate* old_agg = context->aggregates;
		context->aggregates = (Aggregate*)malloc(sizeof(double) * context->agg_slots);
		for (int i = 0; i < context->agg_slots / 2; i++) {
			context->aggregates[i] = old_agg[i];
		}
		free(old_agg);
	}
	int flag = 0;
	if (strcmp(agg.name, "a43") == 0) {
		flag = 1;
	}
	strcpy(context->aggregates[context->agg_num].name, agg.name);
	context->aggregates[context->agg_num].type = agg.type;
	context->aggregates[context->agg_num].value = agg.value;
	context->agg_num++;
}


int add_fetch_result(ClientContext* context, FetchResult* fetch_res) {
	GeneralizedColumnHandle handle;
	strcpy(handle.name, fetch_res->fetch_name);
	handle.generalized_column.column_type = RESULT;
	handle.generalized_column.column_pointer.result = fetch_res;
	if (add_handle(context, handle)) {
		return -1;
	}
	return 0;
}


int add_column_pointer(ClientContext* context, Db* db, Table* table, Column* column) {
	GeneralizedColumnHandle handle;
	char fullname[MAX_SIZE_NAME * 3] = "";
	strcat(fullname, db->name);
	strcat(fullname, ".");
	strcat(fullname, table->name);
	strcat(fullname, ".");
	strcat(fullname, column->name);
	strcpy(handle.name, fullname);
	handle.generalized_column.column_type = COLUMN;
	handle.generalized_column.column_pointer.column.column_data = column;
	handle.generalized_column.column_pointer.column.table = table;

	if (add_handle(context, handle)) {
		return -1;
	}
	return 0;
}


SelectResult get_select_res(ClientContext* context, const char* name) {
	for (int i = 0; i < context->select_num; i++) {
		SelectResult res = context->select_results[i];
		if (strcmp(res.select_name, name) == 0) {
			return res;
		}
	}
}


GeneralizedColumnHandle* get_chandle(ClientContext* context, const char* name) {
	for (int i = 0; i < context->chandles_in_use; i++) {
		GeneralizedColumnHandle* handle = &(context->chandle_table[i]);
		if (strcmp(handle->name, name) == 0) {
			return handle;
		}
	}
	return NULL;
}


Aggregate* get_aggregate(ClientContext* context, const char* name) {
	for (int i = 0; i < context->agg_num; i++) {
		Aggregate* agg = &(context->aggregates[i]);
		if (strcmp(agg->name, name) == 0) {
			return agg;
		}
	}
	return NULL;
}


PrintStruct get_print(ClientContext* context, const char* name) {
	PrintStruct ret = {UNKNOWN, NULL};
	GeneralizedColumnHandle* chandle = get_chandle(context, name);
	Aggregate* agg = get_aggregate(context, name);
	if (chandle == NULL && agg == NULL) {
		return ret;
	} else if (chandle != NULL) {
		ret.type = CHANDLE;
		ret.value.chandle = chandle;
	} else {
		ret.type = AGGREGATE;
		ret.value.aggregate = agg;
	}
	return ret;
}


int clean_chandles(ClientContext* context) {
	if (context->chandle_table != NULL) {
        for (int i = 0; i < context->chandles_in_use; i++) {
            GeneralizedColumnHandle handle = context->chandle_table[i];
            if (handle.generalized_column.column_type == RESULT) {
                int* data_pointer = handle.generalized_column.column_pointer.result->data;
                if (data_pointer != NULL) {
                    free(data_pointer);
                }
            }
        }
        free(context->chandle_table);
    }
	return 0;
}


int clean_select_results(ClientContext* context) {
	if (context->select_results != NULL) {
        for (int i = 0; i < context->select_slots; i++) {
            int* ids = context->select_results[i].id;
            if (ids != NULL) {
                free(ids);
            }
        }
        free(context->select_results);
    }
	return 0;
}


int clean_aggregates(ClientContext* context) {
	if (context->aggregates != NULL) {
		free(context->aggregates);
	}
	return 0;
}


int clean_context(ClientContext* context) {
	if (context != NULL) {
		clean_chandles(context);
		clean_select_results(context);
		clean_aggregates(context);
		free(context);
	}
	return 0;
}

int get_chandle_len(GeneralizedColumnHandle* handle) {
	int n = 0;
	if (handle->generalized_column.column_type == COLUMN) {
		return handle->generalized_column.column_pointer.column.table->table_length;
	} else {
		return handle->generalized_column.column_pointer.result->num_rows;
	}
}

int* get_chandle_data(GeneralizedColumnHandle* handle) {
	if (handle->generalized_column.column_type == COLUMN) {
		int* data = handle->generalized_column.column_pointer.column.column_data->data;
		if (data == NULL) {
			Table* table = handle->generalized_column.column_pointer.column.table;
			Column* column = handle->generalized_column.column_pointer.column.column_data;
			lazy_load(table, column);
		}
		return data;
	} else {
		return handle->generalized_column.column_pointer.result->data;
	}
}
/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
