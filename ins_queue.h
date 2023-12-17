#include <stdatomic.h>
#include <stdint.h>

struct ins_queue_entry{
    void* key;    
    void* value;
};

struct ins_queue{
    volatile _Bool exit;
    uint32_t cap, n_entries;
    struct ins_queue_entry** entries;
};

/* both of these functions busy wait, it may be better to have
 * them use mutex locks since the lock free approach burns too much cpu time
 * it may take away from actual map insertions
 * TODO: make sure to write in cpu saving dynamic usleep()
 * TODO: test map insertions with a mutex version of this
 */
void init_ins_queue(struct ins_queue* iq, int cap);
void insert_ins_queue(struct ins_queue* iq, void* k, void* v);
struct ins_queue_entry* pop_ins_queue(struct ins_queue* iq, uint32_t throttle_after, int usec_sleep);
void abort_ins_queue_operations(struct ins_queue* iq);
/*
 * 
 * take a circular queue,
 * atomically grab next idx, adjust for > cap using % cap
 * atomically grab next idx, 
 * 
 * maybe iterate over whole list, using CAS continuously to see
 * if we have a chance to insert!
*/
