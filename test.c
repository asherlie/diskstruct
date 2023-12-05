#include <string.h>
#include <pthread.h>
#include <stdlib.h>

#include "map.h"

uint16_t hashfnc(void* key){
    return *((int*)key);
}


/* TODO: write a concurrent read + write test */
/* TODO: add #define type code */
/* TODO: add auto parallel insertion, insert just adds elements into a queue where n_threads pop and insert
 *       user will specify n_entries that can be kept in memory which will be an upper bound on n_threads
 *       insertions into this queue will block until there's room - it will store a user specified max
 *       to keep memory usage down
 *       it will have a sane default of 1,000 entries to keep insertions fast
 *       this will still be bottlenecked by n_threads, however
 *
 *       insert_parallel() will block until all insertions are completed
 *       can use mutex waiting to free up cpu for lock free actual insertions
 *
 *       the final insertion can pthread_signal() upon reading an atomic variable
 *       that's incremented after insertion in each thread
 *       even easier would be to use n_entries since it's already atomically set
 */

struct tstr{
    int a;
    char str[6];
    float y;
};

REGISTER_MAP(teststruct, int, struct tstr, hashfnc)
REGISTER_MAP(testmap, int, float, hashfnc)
REGISTER_MAP(intmap, int, int, hashfnc)

/*
 * initialize 10 threads, each is given a diff starting integer
 * check that n_entries is accurate
 * pop all entries to make sure that each integer value is represented
*/
struct parg{
    intmap* m;
    int startpoint, insertions;
};

void* insert_th(void* vparg){
    struct parg* p = vparg;
    for (int i = 0; i < p->insertions; ++i) {
        insert_intmap(p->m, p->startpoint+i, p->startpoint+i);
    }
    return NULL;
}

/* tests if each index up until _ has an integer set that's the same as it */
void test_parallel(int threads, int insertions){
    pthread_t* pth = malloc(sizeof(pthread_t)*threads);
    struct parg* pa;
    int insertions_per_thread = insertions / threads;
    int startpoint = 0;
    int total_entries = 0;
    intmap m;
    init_intmap(&m);

    for (int i = 0; i < threads; ++i) {
        pa = malloc(sizeof(struct parg)*threads);
        pa->m = &m;
        pa->insertions = insertions_per_thread;
        pa->startpoint = startpoint;
        pthread_create(pth+i, NULL, insert_th, pa);
        startpoint += insertions_per_thread;
    }
    for (int i = 0; i < threads; ++i) {
        pthread_join(pth[i], NULL);
    }
    for (int i = 0; i < m.m.n_buckets; ++i) {
        total_entries += m.m.buckets[i].n_entries;
    }
    printf("succesfully inserted %i entries\n", total_entries);
}

void test_struct(){
    teststruct m;
    struct tstr data, ret;
    _Bool found;

    data.a = 94;
    strcpy(data.str, "asher");

    init_teststruct(&m);

    for (int i = 0; i < 10000; ++i) {
        ++data.a;
        insert_teststruct(&m, i, data);
    }
    
    ret = lookup_teststruct(&m, 2795, &found);

    printf("ret.a: %i, ret.str: %s\n", ret.a, ret.str);
}

void test_float(){
    _Bool found;
    testmap m;
    init_testmap(&m);
    insert_testmap(&m, 9904, 32.1);
    printf("%f\n", lookup_testmap(&m, 9904, &found));
}

void test_raw(){
    struct map m;
    int k, v;
    int* lu_v;

    init_map(&m, "ashmap", 10, sizeof(int), sizeof(int), "ashbkt", hashfnc);
    for(int i = 0; i < m.n_buckets; ++i){
        printf("%s\n", m.buckets[i].fn);
    }
    k = 4;
    for (v = 0; v < 200; ++v, ++k) {
        insert_map(&m, &k, &v);
    }

    k = 4922;
    v = 249000;
    insert_map(&m, &k, &v);

    lu_v = lookup_map(&m, &k);
    printf("result: %i\n", *lu_v);
}

int main(){
    test_parallel(1, 100000);
    /*test_struct();*/
    /*test_float();*/
}
