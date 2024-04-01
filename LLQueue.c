#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "HazardPointer.h"
#include "LLQueue.h"

struct LLNode;
typedef struct LLNode LLNode;
typedef _Atomic(LLNode*) AtomicLLNodePtr;

struct LLNode {
    AtomicLLNodePtr next;
    Value item;
};

LLNode* LLNode_new(Value item) {
    LLNode* node = (LLNode*)malloc(sizeof(LLNode));
    atomic_store(&node->next, NULL);
    node->item = item;
    return node;
}


struct LLQueue {
    AtomicLLNodePtr head;
    AtomicLLNodePtr tail;
    HazardPointer *hp;
};

LLQueue* LLQueue_new(void) {
    LLQueue* queue = (LLQueue*)malloc(sizeof(LLQueue));
    LLNode* dummy = LLNode_new(EMPTY_VALUE);
    atomic_store(&queue->head, dummy);
    atomic_store(&queue->tail, dummy);
    queue->hp = malloc(sizeof(HazardPointer));
    HazardPointer_initialize(queue->hp);
    return queue;
}


void LLQueue_delete(LLQueue* queue) {
    LLNode* current = atomic_load(&queue->head);
    while (current != NULL) {
        LLNode* next = atomic_load(&current->next);
        HazardPointer_retire(queue->hp, current);
        current = next;
    }

    HazardPointer_finalize(queue->hp);

    free(queue);
}


void LLQueue_push(LLQueue* queue, Value item) {
    LLNode* new_node = LLNode_new(item); // Tworzymy nowy węzeł.
    LLNode* tail;
    while (true) {
        tail = atomic_load(&queue->tail); // Zabezpieczamy ogon przed zmianą przez inne wątki.
        // "Chronimy" ogon za pomocą Hazard Pointer przed usunięciem.
        HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&queue->tail);

        LLNode* next = atomic_load(&tail->next);
        // Ponownie sprawdzamy, czy ogon się nie zmienił.
        if (tail == atomic_load(&queue->tail)) {
            if (next == NULL) {
                // Próbujemy dodać nowy węzeł na koniec kolejki.
                if (atomic_compare_exchange_strong(&tail->next, &next, new_node)) {
                    // Jeśli się udało, aktualizujemy ogon kolejki.
                    atomic_compare_exchange_strong(&queue->tail, &tail, new_node);
                    // Usuwamy zabezpieczenie dla ogona, ponieważ skończyliśmy operację.
                    HazardPointer_clear(queue->hp);
                    break;
                }
            } else {
                // Jeśli ogon ma już następnik, próbujemy przesunąć ogon dalej.
                atomic_compare_exchange_strong(&queue->tail, &tail, next);
            }
        }
        // Jeśli ogon został zmieniony przez inny wątek, usuwamy zabezpieczenie i próbujemy ponownie.
        HazardPointer_clear(queue->hp);
    }
}

Value LLQueue_pop(LLQueue* queue) {
    LLNode* head;
    LLNode* next;
    Value result;

    while (true) {
        head = atomic_load(&queue->head);
        // Zabezpieczamy 'head' za pomocą Hazard Pointer.
        HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&queue->head);
        next = atomic_load(&head->next);

        if (head != atomic_load(&queue->head)) {
            // Stan kolejki zmienił się, próbujemy ponownie.
            continue;
        }

        if (next == NULL) {
            // Kolejka jest pusta.
            HazardPointer_clear(queue->hp); // Czyszczenie zabezpieczenia.
            return EMPTY_VALUE;
        }

        // Próbujemy zaktualizować 'head' na 'next'.
        if (atomic_compare_exchange_strong(&queue->head, &head, next)) {
            result = next->item;
            // Usuwamy zabezpieczenie dla 'head', ponieważ 'head' już nie jest częścią kolejki.
            HazardPointer_clear(queue->hp);
            // 'head' jest teraz bezużyteczny, więc możemy go bezpiecznie "usunąć".
            HazardPointer_retire(queue->hp, head);
            return result;
        }

        // Czyszczenie zabezpieczenia, jeśli aktualizacja 'head' się nie powiodła.
        HazardPointer_clear(queue->hp);
    }
}


// Value LLQueue_pop(LLQueue* queue)
// {
//     AtomicLLNodePtr head;
//     AtomicLLNodePtr next;
//     Value result;

//     do {
//         head = atomic_load(&queue->head);
//         HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&head);
//         result = atomic_exchange(&head->item, EMPTY_VALUE);
//         if (result != EMPTY_VALUE) {
//             atomic_compare_exchange_weak()
//             return result;
//         } else {
//             if ()
//         }

//     } while (true)

// }

// void* pop(_Atomic Stack* stack) {
//     void* data;
//     Stack next, prev;

//     prev = atomic_load(stack);
//     do {
//         if (prev.head == NULL) {
//             return NULL;
//         }
//         next.head = prev.head->next;
//         next.tag = prev.tag + 1;
//     } while(!atomic_compare_exchange_weak(stack, &prev, next));

//     data = prev.head->data;
//     free(prev.head);

//     return data;
// }




bool LLQueue_is_empty(LLQueue* queue)
{
    LLNode* head = atomic_load(&queue->head);
    // Zabezpieczamy 'head->next' za pomocą Hazard Pointer, aby upewnić się, że jest aktualne.
    HazardPointer_protect(queue->hp, (const _Atomic(void*)*)&head->next);
    LLNode* next = atomic_load(&head->next);

    // Usuwamy zabezpieczenie, ponieważ nie będziemy już korzystać z 'next'.
    HazardPointer_clear(queue->hp);

    // Jeśli 'next' od 'head' jest NULL, kolejka jest pusta.
    return next == NULL;
}

