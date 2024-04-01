#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define MAX_THREADS 128
static const int RETIRED_THRESHOLD = MAX_THREADS;

struct RetiredList {
    void* retired[MAX_THREADS];
    int count;
};

struct HazardPointer {
    _Atomic(void*) pointer[MAX_THREADS];
    struct RetiredList retiredLists[MAX_THREADS];
};
typedef struct HazardPointer HazardPointer;

void HazardPointer_register(int thread_id, int num_threads);
void HazardPointer_initialize(HazardPointer* hp);
void HazardPointer_finalize(HazardPointer* hp);
void* HazardPointer_protect(HazardPointer* hp, const _Atomic(void*)* atom);
void HazardPointer_clear(HazardPointer* hp);
void HazardPointer_retire(HazardPointer* hp, void* ptr);
