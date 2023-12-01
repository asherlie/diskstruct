#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>

#include "map.h"

void create_bucket_file(struct bucket* b){
    (void)b;
}

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, 
              char* bucket_prefix, uint16_t (*hashfunc)(void*)){

    strcpy(m->name, name);
    strcpy(m->bucket_prefix, bucket_prefix);
    m->hashfunc = hashfunc;
    m->key_sz = key_sz;
    m->value_sz = value_sz;
    m->n_buckets = n_buckets;
    m->buckets = calloc(sizeof(struct bucket), n_buckets);
    for(int i = 0; i < m->n_buckets; ++i){
        sprintf(m->buckets[i].fn, "%s_%i.hbk", m->bucket_prefix, i);
    }
}
/* loads map into memory */
void load_map(struct map* m, char* name, uint32_t key_sz, uint32_t value_sz, char* bucket_prefix);
/* k/v size must be consistent with struct map's entries */
_Bool insert_map(struct map* m, void* key, void* value){
    uint16_t idx = m->hashfunc(key) % m->n_buckets;
    struct bucket* b = &m->buckets[idx];
}
void* lookup_map(struct map* m, void* key);

int main(){
    struct map m;
    init_map(&m, "ashmap", 10, sizeof(int), sizeof(int), "ashbkt", NULL);
    for(int i = 0; i < m.n_buckets; ++i){
        printf("%s\n", m.buckets[i].fn);
    }
}
