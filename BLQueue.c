#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "BLQueue.h"
#include "HazardPointer.h"

struct BLNode;
typedef struct BLNode BLNode;
typedef _Atomic(BLNode*) AtomicBLNodePtr;

struct BLNode {
    _Atomic(Value) buffer[BUFFER_SIZE];
    _Atomic(int) push_idx;
    _Atomic(int) pop_idx;
    _Atomic(struct BLNode*) next;
};

// Initialize a new BLNode
static BLNode* BLNode_new(void) {
    BLNode* node = (BLNode*)malloc(sizeof(BLNode));
    if (node != NULL) {
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            atomic_store(&node->buffer[i], EMPTY_VALUE);
        }
        atomic_store(&node->push_idx, 0);
        atomic_store(&node->pop_idx, 0);
        atomic_store(&node->next, NULL);
    }
    return node;
}

struct BLQueue {
    AtomicBLNodePtr head;
    AtomicBLNodePtr tail;
    HazardPointer *hp;
};

BLQueue* BLQueue_new(void) {
    BLQueue* queue = (BLQueue*)malloc(sizeof(BLQueue));
    if (queue != NULL) {
        BLNode* dummy = BLNode_new();
        atomic_store(&queue->head, dummy);
        atomic_store(&queue->tail, dummy);
    }
    queue->hp = malloc(sizeof(HazardPointer));
    HazardPointer_initialize(queue->hp);
    return queue;
}

void BLQueue_delete(BLQueue* queue) {
    BLNode* node;
    while ((node = atomic_load(&queue->head)) != NULL) {
        BLNode* next = atomic_load(&node->next);
        free(node);
        atomic_store(&queue->head, next);
    }
    HazardPointer_finalize(queue->hp);
    free(queue);
}

void BLQueue_push(BLQueue* queue, Value item) {
    BLNode* tail;
    int idx;
    while (true) {
        tail = atomic_load(&queue->tail);

        // Hypothetical call to protect the tail node being accessed
        HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&queue->tail);

        idx = atomic_fetch_add(&tail->push_idx, 1);
        if (idx < BUFFER_SIZE) {
            Value expected = EMPTY_VALUE;
            if (atomic_compare_exchange_strong(&tail->buffer[idx], &expected, item)) {
                // Clear the hazard pointer protection as we're done with the tail node
                HazardPointer_clear(queue->hp);
                return; // Successfully inserted
            }
        } else {
            // Need to allocate a new node
            BLNode* new_tail = BLNode_new();
            if (atomic_compare_exchange_strong(&tail->next, &(BLNode*){NULL}, new_tail)) {
                atomic_store(&queue->tail, new_tail);
                // Clear the hazard pointer protection as we're done with the old tail node
                HazardPointer_clear(queue->hp);
                // Try inserting again
            } else {
                free(new_tail); // Another thread already added a new node
                // Clear the hazard pointer protection as the operation failed and we're retrying
                HazardPointer_clear(queue->hp);
            }
        }
    }
}


Value BLQueue_pop(BLQueue* queue) {
    BLNode* head;
    int idx;
    Value val;
    while (true) {
        head = atomic_load(&queue->head);

        // Hypothetical call to protect the head node being accessed
        HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&queue->head);

        idx = atomic_fetch_add(&head->pop_idx, 1);
        if (idx < BUFFER_SIZE) {
            val = atomic_exchange(&head->buffer[idx], TAKEN_VALUE);
            if (val != EMPTY_VALUE) {
                // Clear the hazard pointer protection as we're done with the head node
                HazardPointer_clear(queue->hp);
                return val; // Successfully popped
            }
            // This slot was empty; trying the next one, so no clear here as we're still in the loop
        } else {
            // Move to the next node
            BLNode* next = atomic_load(&head->next);
            if (next == NULL) {
                // Clear the hazard pointer protection as the queue is empty and we're about to exit
                HazardPointer_clear(queue->hp);
                return EMPTY_VALUE; // Queue is empty
            }
            if (atomic_compare_exchange_strong(&queue->head, &head, next)) {
                // Successfully moved head; now it's safe to free the old head
                // But before freeing, clear the hazard pointer to avoid use-after-free in other threads
                HazardPointer_clear(queue->hp);
                HazardPointer_retire(queue->hp, head);
            }
            // If another thread already updated head, retry without clearing the hazard pointer
            // as we're continuing the loop and still need the protection
        }
    }
}


bool BLQueue_is_empty(BLQueue* queue) {
    BLNode* head;
    bool isEmpty;

    while (true) {
        head = atomic_load(&queue->head);

        // Hypothetically protect the head node being accessed
        HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&queue->head);

        int pop_idx = atomic_load(&head->pop_idx);
        BLNode* next = atomic_load(&head->next);

        // Check if the head node is beyond its capacity and if the next node is NULL
        isEmpty = (pop_idx >= BUFFER_SIZE) && (next == NULL);

        // Clear the hazard pointer protection after checking
        HazardPointer_clear(queue->hp);

        if (isEmpty || next == NULL) {
            break; // Exit the loop if the queue is determined to be empty or if next is NULL
        } else {
            // In case the head has moved, retry the operation
            continue;
        }
    }

    return isEmpty;
}
