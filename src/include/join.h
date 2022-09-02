#ifndef JOIN_H
#define JOIN_H


int nested_looped_join(int* pos1, int* val1, int* pos2, int* val2, int n1, int n2, int* r1, int* r2, int* s1, int* s2);
int hash_join(int* pos1, int* val1, int* pos2, int* val2, int n1, int n2,  int* r1, int* r2, int* s1, int* s2);
#endif