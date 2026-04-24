#include "grpc_ring_loop.h"

#include <pthread.h>
#include <string.h>

#define GRPC_RING_CAPACITY 4096U

static ProtoTask g_task_ring[GRPC_RING_CAPACITY];
static ProtoCompletion g_cpl_ring[GRPC_RING_CAPACITY];

static uint32_t g_task_head = 0;
static uint32_t g_task_tail = 0;
static uint32_t g_task_count = 0;

static uint32_t g_cpl_head = 0;
static uint32_t g_cpl_tail = 0;
static uint32_t g_cpl_count = 0;

static int g_shutdown = 0;

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_task_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t g_task_not_full = PTHREAD_COND_INITIALIZER;
static pthread_cond_t g_cpl_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t g_cpl_not_full = PTHREAD_COND_INITIALIZER;

void grpc_ring_reset(void)
{
    pthread_mutex_lock(&g_lock);
    g_task_head = g_task_tail = g_task_count = 0;
    g_cpl_head = g_cpl_tail = g_cpl_count = 0;
    g_shutdown = 0;
    pthread_cond_broadcast(&g_task_not_full);
    pthread_cond_broadcast(&g_task_not_empty);
    pthread_cond_broadcast(&g_cpl_not_full);
    pthread_cond_broadcast(&g_cpl_not_empty);
    pthread_mutex_unlock(&g_lock);
}

void grpc_ring_set_shutdown(int on)
{
    pthread_mutex_lock(&g_lock);
    g_shutdown = (on != 0);
    pthread_cond_broadcast(&g_task_not_full);
    pthread_cond_broadcast(&g_task_not_empty);
    pthread_cond_broadcast(&g_cpl_not_full);
    pthread_cond_broadcast(&g_cpl_not_empty);
    pthread_mutex_unlock(&g_lock);
}

int grpc_ring_is_shutdown(void)
{
    int ret;
    pthread_mutex_lock(&g_lock);
    ret = g_shutdown;
    pthread_mutex_unlock(&g_lock);
    return ret;
}

int grpc_ring_push_task(const ProtoTask *task)
{
    pthread_mutex_lock(&g_lock);
    while (g_task_count == GRPC_RING_CAPACITY && !g_shutdown)
        pthread_cond_wait(&g_task_not_full, &g_lock);

    if (g_shutdown) {
        pthread_mutex_unlock(&g_lock);
        return 0;
    }

    g_task_ring[g_task_tail] = *task;
    g_task_tail = (g_task_tail + 1U) % GRPC_RING_CAPACITY;
    g_task_count++;

    pthread_cond_signal(&g_task_not_empty);
    pthread_mutex_unlock(&g_lock);
    return 1;
}

int grpc_ring_pop_completion(ProtoCompletion *cpl)
{
    pthread_mutex_lock(&g_lock);
    while (g_cpl_count == 0 && !g_shutdown)
        pthread_cond_wait(&g_cpl_not_empty, &g_lock);

    if (g_cpl_count == 0 && g_shutdown) {
        pthread_mutex_unlock(&g_lock);
        return 0;
    }

    *cpl = g_cpl_ring[g_cpl_head];
    g_cpl_head = (g_cpl_head + 1U) % GRPC_RING_CAPACITY;
    g_cpl_count--;

    pthread_cond_signal(&g_cpl_not_full);
    pthread_mutex_unlock(&g_lock);
    return 1;
}

int ring_pop_task(ProtoTask *task)
{
    int has_task = 0;

    pthread_mutex_lock(&g_lock);
    if (g_task_count > 0) {
        *task = g_task_ring[g_task_head];
        g_task_head = (g_task_head + 1U) % GRPC_RING_CAPACITY;
        g_task_count--;
        pthread_cond_signal(&g_task_not_full);
        has_task = 1;
    }
    pthread_mutex_unlock(&g_lock);

    return has_task;
}

void ring_push_completion(const ProtoCompletion *cpl)
{
    pthread_mutex_lock(&g_lock);
    while (g_cpl_count == GRPC_RING_CAPACITY && !g_shutdown)
        pthread_cond_wait(&g_cpl_not_full, &g_lock);

    if (!g_shutdown) {
        g_cpl_ring[g_cpl_tail] = *cpl;
        g_cpl_tail = (g_cpl_tail + 1U) % GRPC_RING_CAPACITY;
        g_cpl_count++;
        pthread_cond_signal(&g_cpl_not_empty);
    }
    pthread_mutex_unlock(&g_lock);
}

