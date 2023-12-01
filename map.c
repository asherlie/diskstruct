#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>

#include "map.h"

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, char* bucket_prefix){

    strcpy(m->name, name);
    strcpy(m->bucket_prefix, bucket_prefix);
    m->key_sz = key_sz;
    m->value_sz = value_sz;
    m->n_buckets = n_buckets;
    m->buckets = calloc(sizeof(struct bucket), n_buckets);
}
/* loads map into memory */
void load_map(struct map* m, char* name, uint32_t key_sz, uint32_t value_sz, char* bucket_prefix);
/* k/v size must be consistent with struct map's entries */
_Bool insert_map(struct map* m, void* key, void* value);
void* lookup_map(struct map* m, void* key);

int main(){
    return 0;
}
