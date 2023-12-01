#include <stdint.h>
#include <stdio.h>
#include <stdatomic.h>
#include <linux/limits.h>

/*
 * maps will take this structure on disk
 * can fwrite() be used?
 *
 * should i simulate pointers by pointing to a file offset?
 *
 * option a: 
 *  file should be vastly overallocated at first, leaving room for entries
 * option b:
 *  fileS should be created as needed, buckets can be separated by file which makes this much more modular
 *  FILE*s to each bucket will be kept in memory
 *  this will be modular and easy to realloc a single bucket only
 *  there will be a main file that stores struct map info - n_buckets, element_sz, cap for each bucket and sz for each
 *  but the real insertions will be made to data files
 *
 *  these files will all be written to a directory specified when init_fmap() is called
 */

struct entry{
    /* key/value are cast to void* */
    // wait, do we ever need to even store these in memory?
    // no - this can just be what's returned 
    void* key;
    void* value;
};

struct bucket{
    char fn[PATH_MAX];
    /* fp is set to header, threads will open new FPs to do their actual writes
     * a non-NULL fp indicates that the bucket in question has an associated file/has entries already
     */
    // no pointers are kept
    //FILE* header_fp;

    /* this replaces the header in the actual bucket file, n_entries will be made
     * clear upon finding the first key of NULL
     * capacity will be apparent from filesz / (key_sz+value_sz)
     */
    _Atomic uint32_t n_entries;
    _Atomic uint32_t cap;
    _Atomic uint8_t insertions_in_prog;
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

struct map{
    char name[PATH_MAX];
    char bucket_prefix[PATH_MAX-10];
    uint16_t n_buckets;
    uint32_t key_sz, value_sz;

    uint16_t (*hashfunc)(void*);
    /* each struct entry* provides info about a FILE* */
    //struct entry** buckets;

    struct bucket* buckets;
    //char** buckets_fn;
    //FILE** buckets_fp;
};

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, 
              char* bucket_prefix, uint16_t (*hashfunc)(void*));
/* loads map into memory */
void load_map(struct map* m, char* name, uint32_t key_sz, uint32_t value_sz, char* bucket_prefix);
/* k/v size must be consistent with struct map's entries */
_Bool insert_map(struct map* m, void* key, void* value);
void* lookup_map(struct map* m, void* key);
