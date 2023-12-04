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
    return !ftruncate(fileno(fp), grow_to);

}

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, 
              char* bucket_prefix, uint16_t (*hashfunc)(void*)){
    /*FILE* tmp_bucket;*/
    /*uint32_t entrysz = key_sz + value_sz;*/

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
        m->buckets[i].insertions_in_prog = 0;
        /*tmp_bucket = fopen(m->buckets[i].fn, "w");*/
        /*m->buckets[i].cap = 1;*/
        /*ftruncate(fileno(tmp_bucket), entrysz);*/
    }
}
/* loads map into memory */
void load_map(struct map* m, char* name, uint32_t key_sz, uint32_t value_sz, char* bucket_prefix);
/* k/v size must be consistent with struct map's entries */
int insert_map(struct map* m, void* key, void* value){
    uint16_t idx = m->hashfunc(key) % m->n_buckets;
    uint32_t bucket_idx, bucket_cap;
    uint32_t bucket_offset;
    struct bucket* b = &m->buckets[idx];
    FILE* fp;
    int retries = -1;


/*
 *     lookup bucket, grab idx, grow_file() if needed, retry if idx > cap
 *     increment insertions_in_prog, insert, decrement insertions_in_prog
 *     exit
 * 
 *     is it possible that 2 threads will resize simultaneously?
 *     one thread resizes, every other thread gets idx > cap, retries UNTIL cap is updated
 *     so i gues cap should be atomically loaded! right after retry:
*/
    bucket_idx = atomic_fetch_add(&b->n_entries, 1);
    bucket_offset = bucket_idx * (m->key_sz + m->value_sz);
    /* retrying after getting idx for now
     * this way we never have a corrupted n_entries and we can always set it
     * using the above - eventually we'll either reach a point where idx == cap
     * or where we can insert if we keep retrying!
     */
    retry:
    ++retries;
    bucket_cap = atomic_load(&b->cap);
    /* retry */
    if (bucket_idx > bucket_cap) {
        goto retry;
    }
    /* resize bucket */
    if (bucket_idx == bucket_cap) {
    }
    /* insert regularly */
    /* TODO: check for failed fopen() */
    fp = fopen(b->fn, "w");
    fseek(fp, 0L, SEEK_SET);
    /* TODO: think about endianness, would help for compatibility between machines */
    fwrite(key, m->key_sz, 1, fp);
    fwrite(value, m->value_sz, 1, fp);
    /*
     * right now i'm assuming that we need to initialize buckets to nonzero size
     * but it's probably alright to start them at 0 size as well
     * so long as cap and n_entries are set properly
     *     we can just use the logic laid out in map.h - the thread that gets idx == cap does the resizing
     *     all other threads continuously retry
     *     the first insertion thread will get idx == cap upon calling atomic_inc!
     *     no special case needed :)
    */
    return retries;
}
void* lookup_map(struct map* m, void* key);

int main(){
    struct map m;
    init_map(&m, "ashmap", 10, sizeof(int), sizeof(int), "ashbkt", NULL);
    for(int i = 0; i < m.n_buckets; ++i){
        printf("%s\n", m.buckets[i].fn);
    }
}
