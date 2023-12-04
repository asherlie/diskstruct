#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>

#include "map.h"

void create_bucket_file(struct bucket* b){
    (void)b;
}

_Bool grow_file(char* fn, uint32_t grow_to){
    FILE* fp = fopen(fn, "w");
    ftruncate(fileno(fp), grow_to);
}

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, 
              char* bucket_prefix, uint16_t (*hashfunc)(void*)){
    FILE* tmp_bucket;
    uint32_t entrysz = key_sz + value_sz;
    strcpy(m->name, name);
    strcpy(m->bucket_prefix, bucket_prefix);
    m->hashfunc = hashfunc;
    m->key_sz = key_sz;
    m->value_sz = value_sz;
    m->n_buckets = n_buckets;
    m->buckets = calloc(sizeof(struct bucket), n_buckets);
    mkdir(m->name, 0777);
    for(int i = 0; i < m->n_buckets; ++i){
        sprintf(m->buckets[i].fn, "%s/%s_%i.hbk", m->name, m->bucket_prefix, i);
        /*
         * does this work? will it be resized to be nonzero size using above logic
         * if this is true?
         * atomic incrememt, returns n_entries of 0! == cap, tada!
        */
        m->buckets[i].cap = 0;
        m->buckets[i].n_entries = 0;
        /*tmp_bucket = fopen(m->buckets[i].fn, "w");*/
        /*m->buckets[i].cap = 1;*/
        /*ftruncate(fileno(tmp_bucket), entrysz);*/
    }
}
/* loads map into memory */
void load_map(struct map* m, char* name, uint32_t key_sz, uint32_t value_sz, char* bucket_prefix);
/* k/v size must be consistent with struct map's entries */
_Bool insert_map(struct map* m, void* key, void* value){
    uint16_t idx = m->hashfunc(key) % m->n_buckets;
    struct bucket* b = &m->buckets[idx];

    /*
     * right now i'm assuming that we need to initialize buckets to nonzero size
     * but it's probably alright to start them at 0 size as well
     * so long as cap and n_entries are set properly
     *     we can just use the logic laid out in map.h - the thread that gets idx == cap does the resizing
     *     all other threads continuously retry
     *     the first insertion thread will get idx == cap upon calling atomic_inc!
     *     no special case needed :)
    */
    
}
void* lookup_map(struct map* m, void* key);

int main(){
    struct map m;
    init_map(&m, "ashmap", 10, sizeof(int), sizeof(int), "ashbkt", NULL);
    for(int i = 0; i < m.n_buckets; ++i){
        printf("%s\n", m.buckets[i].fn);
    }
}
