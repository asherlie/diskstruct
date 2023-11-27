#include <stdint.h>

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
    
};

struct bucket{
    char* fn;
    FILE* fp;
    /*
     * should this info be stored? may be better to just 
     * keep track of it in file header only
    */
    //int sz, cap;
    /*
     * a bucket consists of many consecutive entries
     * each entry is provided the same number of bytes depending on
     * element_sz
     *
     * the first handful of bytes of each bucket is used to store {n_entries, capacity}
     *
     * if an entry must be inserted into a bucket and n_entries == capacity, ftruncate
     * will be used to grow the bucket file by at least element_sz in order to accomodate a new entry
     *
     * two file pointers will be store for each bucket
     *  one to keep track of the header information
     *  
     *  one pointing to the next available offset for writing an entry, nvm actually, this won't be compatible with atomic
     *  threadsafety
     *
     * just the one at header will be needed
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
     * we may need to do something similar anyway in the event that
     *
     *
     */
};

struct map{
    uint16_t n_buckets;
    uint16_t element_sz;
    /* each struct entry* provides info about a FILE* */
    struct entry** buckets;
    FILE** buckets;
};
