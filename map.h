#include <stdint.h>
#include <stdio.h>
#include <stdatomic.h>
#include <linux/limits.h>
#include <pthread.h>
#include <string.h>

#include "ins_queue.h"

/* TODO: n_buckets should be configurable */
#define N_BUCKETS 10
#define OVW_PRIMS 1000
#define REGISTER_MAP(name, key_type, val_type, ins_threads, expect_duplicates, hash_func) \
    typedef struct {\
        struct map m;\
    }name;\
    void init_##name(name* m){\
        init_map(&m->m, #name, N_BUCKETS, sizeof(key_type), sizeof(val_type), "autobkt", \
                 ins_threads, expect_duplicates ? OVW_PRIMS : 0, hash_func); \
    } \
    int insert_##name(name* m, key_type k, val_type v){ \
        atomic_fetch_add(&m->m.nominal_insertions, 1); \
        return insert_map(&m->m, &k, &v); \
    } \
    void pinsert_##name(name* m, key_type k, val_type v){ \
        atomic_fetch_add(&m->m.nominal_insertions, 1); \
        /*insert_ins_queue(&m->m.iq, &k, &v);*/ \
        pinsert_map(&m->m, &k, &v); \
    } \
    void pinsert_sync_##name(name* m){ \
        sync_pinsertions(&m->m); \
    } \
    void pinsert_stop_threads_##name(name* m){ \
        stop_pinsert_threads(&m->m); \
    } \
    val_type lookup_##name(name* m, key_type k, _Bool* found){ \
        void* tmp = lookup_map(&m->m, &k);\
        val_type ret; \
        memset(&ret, 0, sizeof(val_type)); \
        *found = 0; \
        if (tmp) {\
            ret = *((val_type*)tmp); \
            *found = 1; \
        } \
        return ret; \
    } \
    void load_##name(name* m){ \
        load_map(&m->m, #name, N_BUCKETS, sizeof(key_type), sizeof(val_type), "autobkt", \
                 ins_threads, expect_duplicates ? OVW_PRIMS : 0, hash_func); \
    }


struct bucket{
    char fn[PATH_MAX];

    _Atomic uint32_t n_entries;
    _Atomic uint32_t cap;
    _Atomic uint8_t insertions_in_prog;
    _Atomic _Bool resize_in_prog;
    /*
     * should this info be stored? may be better to just 
     * keep track of it in file header only
    */
    /*
     * a bucket consists of many consecutive entries
     * each entry is provided the same number of bytes depending on
     * element_sz
     *
     * the first handful of bytes of each bucket is used to store {n_entries, capacity}
     * nvm - n_entries isn't needed, it would be too hard to maintain threadsafety
     *  instead we'll use logic below - key of NULL is end of bucket during lookup
     *  we actually don't necessarily need a header at all, we can easily just store this info in struct bucket
     *  in memory only, 
     *    upon startup if we're loading in an already existing map, we can just calculate n_entries
     *    with key_sz, value_sz, and filesz
     *
     * if an entry must be inserted into a bucket and n_entries == capacity, ftruncate
     * will be used to grow the bucket file by at least element_sz in order to accomodate a new entry
     *
     * only a file pointer pointing to header will be needed
     * multiple threads will be able to write concurrently by opening a FP right before writing after calling
     * atomic_increment
     *
     * the only time insertions will NOT be atomic will be when resizing is needed
     * in this case, a lock will be acquired and the bucket in question will be ftruncate()d
     * how do i ensure that all writes in progress have finished before acquiring lock and ftruncate()ing?
     * i think i'll be able to increment an atomic int each time we open an entry-adding FP
     * and decrement the same counter after a write is completed
     *
     * hmm, but a lock is only useful if it's acquired before each atomic write which defeats the purpose of atomic writes
     * pthread_mutex_
     *
     * okay, we likely don't even need a mutex lock actually
     *  safety can be achieved by using the aforementioned atomic counter
     *  if the counter is 0, indicating that no other thread is inserting into this bucket, we know that no other thread will
     *  attempt to insert until we have more available bytes for entry
     *  with this knowledge we can ignore the problem of insertions during resizing
     *
     *  however this still does leave the problem of multiple threads attempting to resize using ftruncate() simultaneously
     *  this can be solved with a mutex lock or a separate atomic variable
     *  this atomic variable will be a boolean - resizing_in_progress
     *
     *
     * we could maybe skip this whole mess by simply putting the responsibility of resizing buckets on the thread who
     * got the value of capacity from atomic_increment(n_entries)
     * other threads may get an idx that's > capacity
     * these threads must continuously retry until they're able to get a valid idx
     * each inserter thread will atomic_load(capacity) before atomic_inc(n_entries) to get idx
     *
     * retry:
     * atomic_load(capacity)
     * atomic_inc(n_entries)
     * if (idx > capacity) {
     *  goto retry
     * }
     * if (idx == capacity) {
     *  check_if_thread_inserting() // uses logic above to ensure that no thread is mid insertion
     *  resize(); // this involves ftruncate() followed by writing to the bucket header
     *            // this will be threadsafe because other threads will be continuously retrying
     *            // due to the idx > capacity condition
     *  atomic_store(capacity, new_cap)
     * }
     * increment_insertion_counter() // uses logic above to indicate an insertion
     * insert()
     * decrement_insertion_counter() // uses logic above to indicate an insertion ending
     *
     * one missing piece is the updating of n_entries in the bucket header
     * is it possible to have this only be an atomic variable?
     * the issue is with lookups, we can potentially just check if key is NULL, though, which
     * would indicate that we've reached the end of our buckets
     *  two things - firstly this won't be a problem. this is only relevant when we're
     *  loading a map into memory from disk which isn't handled yet
     *      although it won't be a problem like i thought below because it'll just be loaded into mem
     *      and no writes are possible in that time
     *      so we can just check for NULL during startup
     *  secondly - 
     *  this actually doesn't work, there's a chance that we've partially written to some bucket idx
     *  and it'll appear NULL
     *  one way we could solve this is just to not allow concurrent reads/writes
     *  but this isn't great
     *  there can also just be a mutex lock used only for r+w, 
     *  can also actually just use insertions_in_prog!
     *      retry until it's 0?
     *      actually - this doesn't help because lookups are pretty involved, we can potentially
     *      just grab bucket, 
     *      OH! good idea, i can wait until insertions_in_prog == 0 and grab bucket->n_entries at that moment
     *      this is the value that will be  used for n_entries for the lookup call
     *          FIRST, GRAB N_ENTRIES ATOMICALLY, THEN wait until there are no pending insertions.
     *          only at this point will we KNOW that >= n_entries are inserted fully
     *          the only issue that could arise is a potential resize. this can be remedied, however,
     *          by incrementing insertions_in_prog POTENTIALLY
     *
     *
     *      IF there are no pending insertions AND we ensure this remains the case 
     *      we grab n_entries using CAS or similar, 
     *  
     * atomic int n_entries can just be set on startup and upon loading into memory of an old map
     * NO NEED TO HAVE IT ON DISK ALWAYS UPDATED
     * IT CAN JUST BE WRITTEN ON SHUTDOWN to make sure data persists between loads into memory/runs of the program
     *
     * okay, so final plan is:
     *  lookup which bucket to insert into
     *  access the relevant struct bucket
     *  grab an insertion idx, run above checks
     *  
     *
     * NOTE:
     *  reads from map will also rely on the insertion counter being zero, otherwise it'll retry
     *  this allows us to only return fully inserted entries
     *
     * we'll use #define to generate structs with proper k/v types
     *  they'll also be used to generate a struct entry uniquely for this struct map type
     *
     *  we don't even need a struct entry actually, we can just generate functions for lookup()
     *  that cast to whatever type they're passed in the init #define
     *  the real lookup function will return a void* or a uint8_t[] of size specified in args
     *
     * wait, we actually don't need this preprocessor nonsense!
     * i can just add fields for the size of k/v in struct map!
     * it'll be set by initialization function 
     * nvm, may still be needed so functions can actually take in the right types
     * and not just void* k / void* v
     */
};

struct parallel_insertion_helper{
    struct ins_queue iq;
    _Atomic _Bool ready;
    int idx_overwrite_prims;
    _Atomic _Bool* idx_overwrite;

    int n_threads;
    pthread_t* pth;
};

struct map{
    char name[PATH_MAX/2];
    char bucket_prefix[(PATH_MAX/2)-10];
    uint16_t n_buckets;
    uint32_t key_sz, value_sz;

    uint16_t (*hashfunc)(void*);

    struct bucket* buckets;

    _Atomic uint32_t nominal_insertions;
    _Atomic uint32_t total_insertions;

    /* this will be initialized in init_map() and will only be used by insert_map_parallel() */
    struct parallel_insertion_helper pih;
};

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, 
              char* bucket_prefix, int n_threads, int idx_overwrite_prims, uint16_t (*hashfunc)(void*));
/* loads map into memory */
void load_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz,
              char* bucket_prefix,  int n_threads, int idx_overwrite_prims, uint16_t (*hashfunc)(void*));
/* k/v size must be consistent with struct map's entries */
int insert_map(struct map* m, void* key, void* value);
void* lookup_map(struct map* m, void* key);

void pinsert_map(struct map* m, void* key, void* value);
void sync_pinsertions(struct map* m);
void stop_pinsert_threads(struct map* m);
