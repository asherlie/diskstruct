// TODO: add this to my pinned repos
#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>

#include "map.h"

_Bool grow_file(char* fn, uint32_t grow_to){
    _Bool ret;
    int fd = open(fn, O_CREAT | O_WRONLY, 0666); 
    ret = !ftruncate(fd, grow_to);
    fsync(fd);
    close(fd);
    return ret;
}

void init_pih(struct parallel_insertion_helper* pih, int queue_cap, int n_threads){
    pih->ready = 0;
    init_ins_queue(&pih->iq, queue_cap);
    pih->n_threads = n_threads;
    pih->pth = malloc(sizeof(pthread_t)*n_threads);
}

void init_map(struct map* m, char* name, uint16_t n_buckets, uint32_t key_sz, uint32_t value_sz, 
              char* bucket_prefix, uint16_t (*hashfunc)(void*)){

    init_pih(&m->pih, 10000, 40);
    m->pih.ready = 0;
    strcpy(m->name, name);
    strcpy(m->bucket_prefix, bucket_prefix);
    m->hashfunc = hashfunc;
    m->key_sz = key_sz;
    m->value_sz = value_sz;
    m->n_buckets = n_buckets;
    m->buckets = calloc(sizeof(struct bucket), n_buckets);
    m->nominal_insertions = m->total_insertions = 0;
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
    int kvsz = m->value_sz + m->key_sz + 1;
    uint8_t lu_kv[kvsz];
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
             * finalized plan is:
             *  make this behave like a true hashmap - map[i] = x; map[i] = y; should result with ONLY map[i] == y
             *
             * then, to fix load(), we will read entries until two consecutive NULL/NULL pairs are found
             * this may, however, lead to an issue where a real NULL/NULL pair is inserted into the last idx
             * of a bucket and is ignored
             *
             * this can just be fixed by having a byte prepended to each bucket idx - it'll be uint8_t,keysz,valsz
             * uint8_t will just be a nonzero char, this way we can guarantee that only once we find a TRUE NULL entry
             * will we be done iterating. as a matter of fact, this should be written before true hashmap functionality
             *
             * a better way would be to have a sealing function that allows reading later
             * this function will just truncate bucket files to their exact size!
             * this way we can just assume cap == n_entries upon loading
             *
             * files may be corrupted, though, if a crash occurs before cleanup
             *  this is a tough problem, for now i'm going to use the truncation design
             *  this allows us to minimize footprint of entries by not adding a byte for each one
             *
             * extra byte:
             *  no need to call a function before exiting after insertion
             *  but requires taking up more space
             *  crashes will not leave file in a bad state, worst case is some garbage entries
             *  from partial insertions
             *
             * sealing function:
             *  file will be unloadable if not sealed, could be in a bad state after crashes
             *  worst case is many NULL/NULL entries
             *
             * could use a combo of both designs for now, add TODOs to remove one
             * when loading, can set cap to size of file, iterate until we find a NULL/NULL, then set
             * sz to i
             * this way it'll be correct nomatter what
             *
             * okay, aside from the above two options, i may have just thought of the perfect solution...
             * is there a reason we can't just have a SEPARATE FILE for each bucket idx? is there overhead associated with many small files vs. 
             * few large ones
             *
             * idx = hash(data)
             * bucket_idx = atomic_()
             * fn = idx_bucket_idx
             *
             * loading becomes trivial because we can just check if a file exists given our naming convention
             * the issue is knowing how high to iterate, some writes may have failed
             * do we stop once we find a gap? a later inserion idx may have completed
             * i guess it's alright to miss a few entries in this edge case
             * the overhead of opening many file descriptors might prove to be too much
             *
             *
             * TODO: see if test is working with 1-indexing!!
             * if not, damn
             *
             * solve this problem first and THEN solve the hashmap reality problem
             *
             * the real difference between the two original solutions is whether or not data will
             * be corrupted during writing for reading
             * with extra byte we can load() while a diff process is writing
             * either way, though, a loading gap will be needed to ensure nothing is corrupted
             * writes are not atomic and resizing certainly isn't either
             *
             *  maybe i can define some kind of primitive to do this, actually i can probably use SYS V messages
             *  we can either insert into a svq as a load() request and wait until this message is removed!
             *      this will allow us to request an insertion pause from any insertion threads that gives
             *      enough time to load()
             *
             *  interesting, could also have a daemon that's always running that contains our thread safety mechanisms
             *  it will hold our structs, this will be a relatively simple change - it'll just move the in-memory portion
             *  of our storage to a process that is started with systemctl
             *  it may be slower unfortunately, though, i'd need to add an interface to talk to this process
             *  maybe not, actually, we could just keep all insertions in that proc, lookup/write will just recv
             *  params needed over a unix socket
             *  we can then just actually seal before load(), load() will just be what this process does on startup
             *
             *      init_map() can maybe fork() and create this process, each unique map will create a new process
             *
             *  maybe scrap all of the above, write a load_map_readonly() function that allows us to load
             *  map into "memory" and upon each lookup we can check for NULL/NULL. this is the best solution
             *  because it allows us to load as we go with no resizing needed
             *  the only issue is partial writes
             *  but this can be remedied with the same thing mentioned above, svq load requests
             *  not so elegant though 
             *  maybe a better strat would be to use unix sockets to communicate n_entries of each bucket
             *  nvm - 
             *      so, i should use the extra byte strat and always ensure that there's a NULL termination
             *      after each bucket, this will be easy - i can just always overalloc by 1 entry
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
    uint16_t idx = m->hashfunc(key) % m->n_buckets, entrysz = m->key_sz + m->value_sz + 1;
    uint32_t bucket_idx, bucket_cap;
    uint32_t bucket_offset;
    struct bucket* b = &m->buckets[idx];
    int fd;
    int retries = -1;
    const uint8_t filler_byte = 'Z';


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
    bucket_offset = bucket_idx * entrysz;
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
        /*wow, are we using int bucket_cap when it should be bytges bucket cap?*/
        uint32_t grow_to = (bucket_cap ? bucket_cap * 2 : 1) * entrysz;
        /*
         * i think the problem is that we're resizing too late
         * and writing garbage to after the end of our file
         * which is somehow working but is zeroed after grow_to()
        */
        printf("growing file to %i bytes\n", grow_to);
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
    write(fd, &filler_byte, 1);
    write(fd, key, m->key_sz);
    write(fd, value, m->value_sz);
    fsync(fd);
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
    atomic_fetch_add(&m->total_insertions, 1);
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
        // no need to seek, read()ing is seeking
        /*lseek(fd, i * (m->key_sz + m->value_sz + 1), SEEK_SET);*/
        read(fd, lu_key, 1);
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

/* TODO: remove all of the below and put into a different thread specific file */

/* this thread will exit once m->pih.iq.exit is set */
void* parallel_insertion_thread(void* vmap){
    struct map* m = vmap;
    struct ins_queue_entry* iqe = (void*)0x1;
    while ((iqe = pop_ins_queue(&m->pih.iq, 100, 1000))) {
        insert_map(m, iqe->key, iqe->value);
        free(iqe);
    }
    return NULL;
}

/* spawns n_threads threads */
void maybe_spawn_pinsert_threads(struct map* m){
    _Bool expected = 0;
    /* exit if pih.ready is already 1
     * it's not important that threads are actually spawned if another thread
     * pinserts before the insertion threads ready to actually insert because the queue will
     * just begin to fill up, the only thing that's important to guarantee is that only
     * one thread spawns the threads
     */
    if (!atomic_compare_exchange_strong(&m->pih.ready, &expected, 1)) {
        return;
    }
    for (int i = 0; i < m->pih.n_threads; ++i) {
        pthread_create(m->pih.pth + i, NULL, parallel_insertion_thread, m);
    }
}

void sync_pinsertions(struct map* m){
    uint32_t nom_ins = atomic_load(&m->nominal_insertions);
    while (atomic_load(&m->total_insertions) < nom_ins) {
        ;
    }
}

void pinsert_thread_cleanup(struct map* m){
    for (int i = 0; i < m->pih.n_threads; ++i) {
        pthread_join(m->pih.pth[i], NULL);
    }
    free(m->pih.pth);
    free_ins_queue(&m->pih.iq);
}

/* init_ins_queue can be recalled after this to resume regular operation */
/*can this have an option to reset and declare a new number of threads?
 * best not to i think, this is an edge use case and the user can achieve
 * this with the current impl. just by using init_ins_queue()
 */
void stop_pinsert_threads(struct map* m){
    m->pih.iq.exit = 1;
    pinsert_thread_cleanup(m);
}

/* spawns popping threads if !ready, inserts into ins_queue */
void pinsert_map(struct map* m, void* key, void* value){
    maybe_spawn_pinsert_threads(m);
    insert_ins_queue(&m->pih.iq, key, value);
}
