// TODO: abstract this to just be called a lock free circular queue
// TODO: write macros to define functions for a specific type
// TODO: make this a seaparate library on github
// TODO: if this ends up being cool, add it to my pinned repos!
#include <stdlib.h>
#include <unistd.h>
#include <stdatomic.h>

#include "ins_queue.h"

/* cap will never ben expanded */
void init_ins_queue(struct ins_queue* iq, int cap){
    iq->exit = 0;
    iq->n_entries = 0;
    iq->cap = cap;
    iq->entries = calloc(sizeof(struct ins_queue_entry*), cap);
    return;
}

void insert_ins_queue(struct ins_queue* iq, void* k, void* v){
    struct ins_queue_entry* niqe,
           * iqe = malloc(sizeof(struct ins_queue_entry));

    iqe->key = k;
    iqe->value = v;

    while (!iq->exit) {
        /*
         * instead of this we can atomically grab indices
         * one at a time, after grabbing, % cap!
        */
        for (uint32_t i = 0; i < iq->cap; ++i) {
            niqe = NULL;
            if (atomic_compare_exchange_strong(&iq->entries[i], &niqe, iqe)) {
                return;
            }
        }
    }
    return;
}

/* this function will gradually begin to sleep
 * once n full iterations have failed to pop
 * and sleep will cease once one valid read comes in
 *
 * this also should have a forced exit mechanism
 *
 * pop_ins_queue() busy waits for an entry
 * after n attempts, it will begin to sleep between full iterations
 */
struct ins_queue_entry* pop_ins_queue(struct ins_queue* iq, uint32_t throttle_after, int usec_sleep){
    struct ins_queue_entry* niqe = NULL, * ret;
    uint32_t attempts = 0;

    while (!iq->exit) {
        if (attempts++ > throttle_after) {
            usleep(usec_sleep);
        }
        for (uint32_t i = 0; i < iq->cap; ++i) {
            ret = atomic_exchange(iq->entries + i, niqe);
            if (ret) {
                return ret;
            }
        }
    }
    return NULL;
}

/* this function isn't great, it doesn't wait until all threads stop
 * their operations
 */
void abort_ins_queue_operations(struct ins_queue* iq){
    iq->exit = 1;
}
