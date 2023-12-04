#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>

#include "map.h"

_Bool grow_file(char* fn, uint32_t grow_to){
    _Bool ret;
    FILE* fp = fopen(fn, "w");
    ret = !ftruncate(fileno(fp), grow_to);
    fclose(fp);
    return ret;
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
        uint32_t grow_to = bucket_cap ? bucket_cap * 2 : m->value_sz + m->key_sz;
        /* wait until all current insertions are finished / all FILE*s are closed
         * before beginning file resize
         */
        while (atomic_load(&b->insertions_in_prog)) {
            ;
        }
        /* lookup_map() will wait for this flag to be cleared */
        atomic_flag_test_and_set(&b->resize_in_prog);
        grow_file(b->fn, grow_to);
        atomic_flag_clear(&b->resize_in_prog);
        atomic_store(&b->cap, grow_to);
    }
    atomic_fetch_add(&b->insertions_in_prog, 1);
    /* insert regularly */
    /* TODO: check for failed fopen() */
    fp = fopen(b->fn, "w");
    fseek(fp, bucket_offset, SEEK_SET);
    /* TODO: think about endianness, would help for compatibility between machines */
    fwrite(key, m->key_sz, 1, fp);
    fwrite(value, m->value_sz, 1, fp);
    fclose(fp);
    /* this is only relevant for resizing of buckets which is why we decrement AFTER fclose()
     * concurrent writes to different offsets of one bucket are perfectly fine
     */
    atomic_fetch_sub(&b->insertions_in_prog, 1);
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

void* lookup_map(struct map* m, void* key){
    uint16_t idx = m->hashfunc(key) % m->n_buckets;
    struct bucket* b = &m->buckets[idx];
    uint32_t n_entries = atomic_load(&b->n_entries);
    _Bool insertions_completed = 0, resize_in_prog = 1;
    FILE* fp;
    void* lu_value = malloc(m->value_sz);
    void* lu_key = malloc(m->key_sz);

    /* incrementing insertions_in_prog before waiting until all insertions are finished
     * incrementing is done to ensure that no resizing will begin during our lookup
     *
     * there's no guarantee that more insertions won't begin after we've read insertions_in_prog of 1, but
     * this doesn't matter because at this point we will have a guarantee of >= n_entries entries being written
     */
    /*what if resize is in progress as we start*/
    atomic_fetch_add(&b->insertions_in_prog, 1);

    /* after ensuring we will not begin a bucket resize, we need to wait until
     * a potential running resize is complete
     * this is done in the same loop that we wait until n_entries are guaranteed to be populated
     */

    /* TODO: replace with do while */
    while (!insertions_completed || resize_in_prog) {
        if (atomic_load(&b->insertions_in_prog) == 1){
            insertions_completed = 1;
        }
        /* once resize in progress has completed, we know that another resize will not occur
         * until we decrement insertions_in_prog so we're safe to access the bucket in question
         */
        if (!atomic_flag_test_and_set(&b->resize_in_prog)) {
            resize_in_prog = 0;
        }
    }

    fp = fopen(b->fn, "r");

    /* TODO: allow loading of a specified number of k/v pairs into memory to speed up lookups */

    for (uint32_t i = 0; i < n_entries; ++i) {
        fseek(fp, i * (m->key_sz + m->value_sz), SEEK_SET);
        fread(lu_key, m->key_sz, 1, fp);
        fread(lu_value, m->value_sz, 1, fp);
        if (!memcmp(key, lu_key, m->key_sz)) {
            break;
        }
    }

    atomic_fetch_sub(&b->insertions_in_prog, 1);

    free(lu_key);

    return lu_value;
}
