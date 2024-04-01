#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>

#include <stdio.h>
#include <stdlib.h>

#include "HazardPointer.h"
#include "RingsQueue.h"

struct RingsQueueNode;
typedef struct RingsQueueNode RingsQueueNode;

struct RingsQueueNode {
    _Atomic(RingsQueueNode*) next;
    Value buffer[RING_SIZE];
    atomic_size_t push_idx;
    atomic_size_t pop_idx;
};

RingsQueueNode* RingsQueueNode_new(void) {
    RingsQueueNode* node = (RingsQueueNode*)malloc(sizeof(RingsQueueNode));
    atomic_init(&node->next, NULL);
    atomic_init(&node->push_idx, 0);
    atomic_init(&node->pop_idx, 0);
    // Inicjalizacja bufora może być pominięta, ponieważ używamy indeksów do zarządzania dostępem
    return node;
}

struct RingsQueue {
    RingsQueueNode* head;
    RingsQueueNode* tail;
    pthread_mutex_t pop_mtx;
    pthread_mutex_t push_mtx;
};

RingsQueue* RingsQueue_new(void)
{
    RingsQueue* queue = (RingsQueue*)malloc(sizeof(RingsQueue));
    if (queue == NULL) {
        return NULL;
    }
    RingsQueueNode* sentinel = RingsQueueNode_new();
    if (sentinel == NULL) {
        free(queue);
        return NULL;
    }
    queue->head = sentinel;
    queue->tail = sentinel;
    pthread_mutex_init(&queue->pop_mtx, NULL);
    pthread_mutex_init(&queue->push_mtx, NULL);
    return queue;
}

void RingsQueue_delete(RingsQueue* queue)
{
    RingsQueueNode* current = queue->head;
    while (current != NULL) {
        RingsQueueNode* next = atomic_load(&current->next);
        free(current);
        current = next;
    }
    pthread_mutex_destroy(&queue->pop_mtx);
    pthread_mutex_destroy(&queue->push_mtx);
    free(queue);
}

void RingsQueue_push(RingsQueue* queue, Value item)
{
    pthread_mutex_lock(&queue->push_mtx);
    RingsQueueNode* tail = queue->tail;
    size_t push_idx = atomic_load(&tail->push_idx);
    if (push_idx < RING_SIZE) {
        tail->buffer[push_idx] = item;
        atomic_fetch_add(&tail->push_idx, 1);
    } else {
        RingsQueueNode* new_node = RingsQueueNode_new();
        new_node->buffer[0] = item;
        atomic_store(&new_node->push_idx, 1);
        atomic_store(&tail->next, new_node);
        queue->tail = new_node;
    }
    pthread_mutex_unlock(&queue->push_mtx);
}

Value RingsQueue_pop(RingsQueue* queue)
{
    pthread_mutex_lock(&queue->pop_mtx);
    RingsQueueNode* head = queue->head;
    size_t pop_idx = atomic_load(&head->pop_idx);
    if (pop_idx < atomic_load(&head->push_idx)) {
        Value item = head->buffer[pop_idx];
        atomic_fetch_add(&head->pop_idx, 1);
        pthread_mutex_unlock(&queue->pop_mtx);
        return item;
    } else if (atomic_load(&head->next) != NULL) {
        RingsQueueNode* next = atomic_load(&head->next);
        queue->head = next;
        free(head);
        pthread_mutex_unlock(&queue->pop_mtx);
        return RingsQueue_pop(queue); // Rekurencyjne wywołanie dla nowego head
    }
    pthread_mutex_unlock(&queue->pop_mtx);
    return EMPTY_VALUE;
}

bool RingsQueue_is_empty(RingsQueue* queue)
{
    pthread_mutex_lock(&queue->pop_mtx);
    bool is_empty = (queue->head == queue->tail) && (atomic_load(&queue->head->pop_idx) == atomic_load(&queue->head->push_idx));
    pthread_mutex_unlock(&queue->pop_mtx);
    return is_empty;
}
