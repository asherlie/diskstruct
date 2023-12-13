#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "map.h"

_Bool grow_file(char* fn, uint32_t grow_to){
    _Bool ret;
    int fd = open(fn, O_TRUNC | O_CREAT | O_WRONLY, 0666);
    ret = !ftruncate(fd, grow_to);
    close(fd);
    return ret;
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
    /*does this overwrite if already exists?*/
    mkdir(m->name, 0777);
    for(int i = 0; i < m->n_buckets; ++i){
        sprintf(m->buckets[i].fn, "%s/%s_%i.hbk", m->name, m->bucket_prefix, i);
        m->buckets[i].cap = 0;
        m->buckets[i].n_entries = 0;
        m->buckets[i].insertions_in_prog = 0;
        m->buckets[i].resize_in_prog = 0;
    }
}

/* these two functions are not thread safe and thus should only
 * be called on startup
 */
void get_bucket_info(struct map* m, struct bucket* b){
    int fd = open(b->fn, O_RDONLY);
    int kvsz = m->value_sz + m->key_sz;
    void* lu_kv = malloc(kvsz);
    uint8_t kvzero[kvsz];

    /* some buckets may not have been created yet */
    if (fd == -1) {
        return;
    }

    memset(kvzero, 0, kvsz);
    b->cap = lseek(fd, 0, SEEK_END) / kvsz;
    lseek(fd, 0, SEEK_SET);
    for (uint32_t i = 0; i < b->cap; ++i) {
        memset(lu_kv, 0, kvsz);
        printf("read returned: %li\n", read(fd, lu_kv, kvsz));
        if (!memcmp(lu_kv, kvzero, kvsz)) {
            /*
             * AHA! i think there's not a bug it's just written badly!!
             * obviously this isn't a good way to detect a problem!
             * if key and value are integers and are set to 0 then it'll appear NULL!!
             * test this by setting key or value to nonzero!!
            */
            /* this is just about solved, now i just need a solution LOL
             * easy! two consecutive NULL/NULL pairs will indicate the end!
             * this will never occur in reality, as there cannot be two identical entries
             * oh, actually there can be
             * there's no duplicate checking
             *
             * could always add another byte but would be very inefficient to do so for every
             * bucket
             *
             * it'd be nice if this was implicit
             * if we could create some pattern during insertion
             *
             * actually! the above would work, but not just two consecutive entries!
             * we must reach a point where all the remaining entries are NULL/NULL pairs
             * only then will we know
             *
             * actually one last time, this is all stupid. if this behaved like a real hashmap we would
             * not have this issue.
             * we should have proper behavior of only one entry per key
             * in this case, there will be only one N/N
             * overwriting must be possible
             *
             * this is doable but difficult, each bucket (or bucket offset even?) should have its own 
             * atomic variable that we use to spinlock in the event of an ovewrite
             * can we reuse an existing var?
             * ugh, each insertion also must check for an existing key
             * we could set a variable to check dupes which will allow much faster insretions if disabled
             *
             */
            printf("found a NULL entry at idx %i for bucket: \"%s\"\n", i, b->fn);
            break;
        }
        ++b->n_entries;
        /*read key and value, if they're NULL, set n_entries!*/
        /*
         * or even just if key is NULL! a NLL value can be valid
         * could check both to be safe
         * once we can load them in, abstract it in a define
         * test it!
         * this just became VERY flexible
        */
    }
    close(fd);
}

/* loads map into "memory" */
// TODO: define this in #define as well
void load_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz,
              char* bucket_prefix,  uint16_t (*hashfunc)(void*)){
    init_map(m, name, n_buckets, key_sz, value_sz, bucket_prefix, hashfunc);
    for (int i = 0; i < m->n_buckets; ++i) {
        get_bucket_info(m, &m->buckets[i]);
    }
}

/* k/v size must be consistent with struct map's entries */
int insert_map(struct map* m, void* key, void* value){
    uint16_t idx = m->hashfunc(key) % m->n_buckets;
    uint32_t bucket_idx, bucket_cap;
    uint32_t bucket_offset;
    struct bucket* b = &m->buckets[idx];
    int fd;
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
        atomic_store(&b->resize_in_prog, 1);
        grow_file(b->fn, grow_to);
        atomic_store(&b->resize_in_prog, 0);
        atomic_store(&b->cap, grow_to);
    }
    atomic_fetch_add(&b->insertions_in_prog, 1);
    /* insert regularly */
    /* TODO: check for failed fopen() */
    fd = open(b->fn, O_WRONLY);
    lseek(fd, bucket_offset, SEEK_SET);
    /* TODO: think about endianness, would help for compatibility between machines */
    write(fd, key, m->key_sz);
    write(fd, value, m->value_sz);
    close(fd);
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
    _Bool insertions_completed = 0, resize_in_prog = 1, found = 0;
    int fd;
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
        if (!atomic_load(&b->resize_in_prog)) {
            resize_in_prog = 0;
        }
    }

    fd = open(b->fn, O_RDONLY);

    /* TODO: allow loading of a specified number of k/v pairs into memory to speed up lookups */

    for (uint32_t i = 0; i < n_entries; ++i) {
        lseek(fd, i * (m->key_sz + m->value_sz), SEEK_SET);
        read(fd, lu_key, m->key_sz);
        read(fd, lu_value, m->value_sz);
        if (!memcmp(key, lu_key, m->key_sz)) {
            found = 1;
            break;
        }
    }

    atomic_fetch_sub(&b->insertions_in_prog, 1);
    close(fd);

    free(lu_key);
    if (!found) {
        free(lu_value);
        lu_value = NULL;
    }

    return lu_value;
}
