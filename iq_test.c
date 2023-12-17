#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include "ins_queue.h"

struct ins_queue iq;
int ipt = 200;

void* ins_thread(void* varg){
    int a, b;
    (void)varg;
    for (int i = 0; i < ipt; ++i) {
        insert_ins_queue(&iq, &a, &b);
    }
    return NULL;
}

int main(){
    int n_therads = 100;
    int iq_cap = 300;
    int n_popped = 0;

    pthread_t* pth = malloc(sizeof(pthread_t)*n_therads);
    init_ins_queue(&iq, iq_cap);

    for (int i = 0; i < n_therads; ++i) {
        pthread_create(pth+i, NULL, ins_thread, NULL);
    }

    while (n_popped != n_therads * ipt) {
        pop_ins_queue(&iq, 100, 1000);
        ++n_popped;
        printf("\rpopped %i entries", n_popped);
    }
    puts("");
}
