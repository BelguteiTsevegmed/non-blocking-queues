#include "HazardPointer.h"
#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>

thread_local int _thread_id = -1;
int _num_threads = -1;

void HazardPointer_register(int thread_id, int num_threads)
{
    _thread_id = thread_id;
    _num_threads = num_threads;
}

void HazardPointer_initialize(HazardPointer* hp)
{
    for (int i = 0; i < MAX_THREADS; ++i) {
        atomic_store(&hp->pointer[i], NULL);
        hp->retiredLists[i].count = 0;
    }
}

void HazardPointer_finalize(HazardPointer* hp)
{
    for (int i = 0; i < MAX_THREADS; ++i) {
        for (int j = 0; j < hp->retiredLists[i].count; ++j) {
            free(hp->retiredLists[i].retired[j]);
        }
        hp->retiredLists[i].count = 0;
    }
}

void* HazardPointer_protect(HazardPointer* hp, const _Atomic(void*)* atom)
{
    void* ptr = NULL;
    do {
        ptr = atomic_load(atom);
        atomic_store(&hp->pointer[_thread_id], ptr);
    } while (ptr != atomic_load(atom));
    return ptr;
}

void HazardPointer_clear(HazardPointer* hp)
{
    atomic_store(&hp->pointer[_thread_id], NULL);
}

void HazardPointer_retire(HazardPointer* hp, void* ptr)
{
    int tid = _thread_id;
    hp->retiredLists[tid].retired[hp->retiredLists[tid].count++] = ptr;

    if (hp->retiredLists[tid].count >= RETIRED_THRESHOLD) {
        for (int i = 0; i < hp->retiredLists[tid].count; ++i) {
            void* rp = hp->retiredLists[tid].retired[i];
            bool safeToDelete = true;

            for (int j = 0; j < _num_threads; ++j) {
                if (atomic_load(&hp->pointer[j]) == rp) {
                    safeToDelete = false;
                    break;
                }
            }

            if (safeToDelete) {
                free(rp);
                hp->retiredLists[tid].retired[i] = NULL;
            }
        }

        // Compact the retired list.
        int shift = 0;
        for (int i = 0; i < hp->retiredLists[tid].count; ++i) {
            if (hp->retiredLists[tid].retired[i] == NULL) {
                shift++;
            } else if (shift > 0) {
                hp->retiredLists[tid].retired[i - shift] = hp->retiredLists[tid].retired[i];
                hp->retiredLists[tid].retired[i] = NULL;
            }
        }
        hp->retiredLists[tid].count -= shift;
    }
}
