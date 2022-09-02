#ifndef IO_H
#define IO_H
#include "cs165_api.h"

int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf);
int rmrf(char *path);
int safe_mkdir(const char* path);
int memcopy(int* dest, int* src, int len);
int* map_file(int f, int len);
int unmap_file(int* mapped, int f, int len);
int satisfy_high(int v, long max);
int satisfy_low(int v, long min);
int satisfy(int v, Comparator cmp);
int** load_csv(const char* path, const char* table_name, char** col_names, int* num_cols, int* num_rows);

#endif