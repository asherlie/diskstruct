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
     */
};

struct map{
    uint16_t n_buckets;
    uint16_t element_sz;
    /* each struct entry* provides info about a FILE* */
    struct entry** buckets;
    FILE** buckets;
};
