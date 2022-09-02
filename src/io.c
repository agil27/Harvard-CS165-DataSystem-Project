#include "io.h"
#include <limits.h>
#define MAX_BUF_SIZE 65536
#define MAX_SINGLE_SIZE 192
#include <utils.h>


int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    int rv = remove(fpath);

    if (rv)
        perror(fpath);

    return rv;
}


int rmrf(char *path) {
    return nftw(path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
}


int safe_mkdir(const char* path) {
	struct stat st = {0};
	if (stat(path, &st) == -1) {
    	if (mkdir(path, S_IRWXU)) {
			return -1;
		}
	}
	return 0;
}


int memcopy(int* dest, int* src, int len) {
	for (int i = 0; i < len; i++) {
		dest[i] = src[i];
	}
	return 0;
}

int* map_file(int f, int len) {
	if (f == -1) {
		return 0;
	}

	if (lseek(f, len * sizeof(int), SEEK_SET) == -1) {
		close(f);
		return 0;
	}

	if (write(f, "", 1) == -1) {
		close(f);
		return 0;
	}

	int* mapped = (int*)mmap(0, (len * sizeof(int)), PROT_READ|PROT_WRITE, MAP_SHARED, f, 0);
	if (mapped == MAP_FAILED) {
		close(f);
		return 0;
	}
	return mapped;
}


int unmap_file(int* mapped, int f, int len) {
	if (mapped != NULL && f != -1) {
		if (msync(mapped, len * sizeof(int), MS_SYNC) == -1 || munmap(mapped, len * sizeof(int)) == -1) {	
			return -1;	
		}
	} else {
		return -1;
	}
	close(f);
	return 0;
}


int satisfy_high(int v, long max) {
	if (max == LONG_MIN) {
		return 1;
	} else {
		return (v < (int)(max));
	}
}

int satisfy_low(int v, long min) {
	if (min == LONG_MAX) {
		return 1;
	} else {
		return (v >= (int)(min));
	}
}

int satisfy(int v, Comparator cmp) {
	long x = cmp.p_high;
	long y = cmp.p_low;
	return (satisfy_high(v, x) && satisfy_low(v, y));
}


int read_csv_header(const char* line, const char* table_name, char** result, int* num_cols) {
	char* pure = trim_whitespace(line);
	pure = trim_newline(pure);
	char** ref = &pure;
	char* single;
    int i = 0;
	// char** result = (char**)malloc(sizeof(char*) * MAX_COL);
	while ((single = strsep(ref, ",")) != NULL) {
		result[i++] = (char*)malloc(sizeof(single));
        char** single_ref = &single;
        char* db_name = strsep(single_ref, ".");
        strcpy(table_name, strsep(single_ref, "."));
		strcpy(result[i - 1], *single_ref);
	}
	*num_cols = i;
	return 0;
}


int read_csv_line(const char* line, int** result, int j, int* num_cols) {
	char* pure = trim_whitespace(line);
	pure = trim_newline(pure);
	char** ref = &pure;
	char* single;
	int i = 0;
	// char** result = (char**)malloc(sizeof(char*) * MAX_COL);
	while ((single = strsep(ref, ",")) != NULL) {
		result[i++][j] = atoi(single);
	}
	*num_cols = i;
	return 0;
}


int get_line_count(const char* path) {
	FILE* fp;
	fp = fopen(path, "r");
	char buffer[MAX_BUF_SIZE] = "";
	int res = 0;
	while ((fgets(buffer, MAX_BUF_SIZE, fp)) != NULL) {
		res++;
	}
	fclose(fp);
	return res;
}


int** load_csv(const char* path, const char* table_name, char** col_names, int* num_cols, int* num_rows) {
	*num_rows = get_line_count(path) - 1;
    printf("num rows: %d\n", *num_rows);
	FILE* fp;
	fp = fopen(path, "r");
	if (fp == NULL) {
		return NULL;
	}
	
	char db_name[MAX_SIZE_NAME] = "";

	char buffer[MAX_BUF_SIZE] = "";
	fgets(buffer, MAX_BUF_SIZE, fp);
	*num_cols = 0;
	read_csv_header(buffer, table_name, col_names, num_cols);
	int** cols = (int**)malloc(sizeof(int*) * (*num_rows));
    for (int i = 0; i < *num_cols; i++) {
        cols[i] = (int*)malloc(sizeof(int) * (*num_rows));
    }
	for (int j = 0; j < *num_rows; j++) {
		char line_buffer[MAX_BUF_SIZE];
		fgets(line_buffer, MAX_BUF_SIZE, fp);
		int num_cols_temp = 0;
		read_csv_line(line_buffer, cols, j, &num_cols_temp);
	}
	fclose(fp);
	return cols;
}