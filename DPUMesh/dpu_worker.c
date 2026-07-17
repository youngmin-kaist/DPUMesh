#include "object.h"
#include "comch_server.h"
#include "common.h"
#include "dpa.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "dma.h"
#include "config.h"
#include "comch_msgq.h"
#include "ring.h"
#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <sys/epoll.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

DOCA_LOG_REGISTER(DPU_WORKER);

/* Max consumer-PE events processed per loop iteration. Bounds the drain so the
 * throughput report and control-path advance still run under sustained load. */
#define DATA_DRAIN_BUDGET 8192

/*
 * Baseline (busy-poll) DPU worker.
 *
 * Multi-connection: the shared infrastructure (DPA instance + thread pool,
 * consumer PE, DMA engine) is built once up front; every new host connection
 * is then bound to a slot by the connection callback and driven through its
 * own state machine by dmesh_doca_ctrl_advance(). Both PEs are busy-polled.
 */
void
run_dpu_worker(struct objects *objs, const char *server_name)
{
    doca_error_t result;
    enum dmesh_doca_init_state state;

    DOCA_LOG_INFO("Starting DPU worker %d (busy-poll), server '%s'", objs->worker_idx, server_name);

    /* start the control-path server (non-blocking; creates objs->pe) */
    result = start_comch_ctrl_path_server(server_name, objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start comch control path server: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* build the shared infrastructure before any connection is served */
    result = dmesh_doca_ctrl_advance(objs, &state);
    if (result != DOCA_SUCCESS || state == DMESH_DOCA_STATE_ERROR) {
        DOCA_LOG_ERR("Failed to init shared infrastructure: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    while (true) {
        doca_pe_progress(objs->pe);
        doca_pe_progress(objs->consumer_pe);

        result = dmesh_doca_ctrl_advance(objs, &state);
        if (result != DOCA_SUCCESS)
            DOCA_LOG_ERR("Control-path advance failed: %s", doca_error_get_descr(result));
    }

argp_cleanup:
    return;
}

/*
 * Event-driven (on-demand) DPU worker.
 *
 * Registers the notification fds of BOTH progress engines with epoll: the
 * control PE (connections + metadata messages, feeding the per-connection
 * state machines) and the consumer PE (data path: DPA msgq + DMA completions).
 * The process sleeps until either PE signals, so idle connections cost no CPU.
 * The setup sequencing lives in the shared dmesh_doca_ctrl_advance() state
 * machine (comch_server.c), mirroring what the Rust AsyncFd driver will do
 * via FFI.
 */
void
run_dpu_worker_event_driven(struct objects *objs, const char *server_name)
{
    doca_error_t result;
    enum dmesh_doca_init_state state;
    int ctrl_fd, data_fd, epfd;
    struct epoll_event ev;

    DOCA_LOG_INFO("Starting DPU worker %d (event-driven), server '%s'", objs->worker_idx, server_name);

    /* Start the control-path server without blocking on the host connection. */
    result = start_comch_ctrl_path_server(server_name, objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start comch control path server: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* Build the shared infrastructure (DPA pool, consumer PE, DMA engine)
     * before serving connections; this also makes the consumer PE fd
     * available for epoll registration below. */
    result = dmesh_doca_ctrl_advance(objs, &state);
    if (result != DOCA_SUCCESS || state != DMESH_DOCA_STATE_RUNNING) {
        DOCA_LOG_ERR("Failed to init shared infrastructure: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* Register both PE notification fds with epoll. */
    result = dmesh_doca_ctrl_get_fd(objs, &ctrl_fd);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get control PE notification fd: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    result = doca_pe_get_notification_handle(objs->consumer_pe,
                                             (doca_notification_handle_t *)&data_fd);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer PE notification fd: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    epfd = epoll_create1(0);
    if (epfd < 0) {
        DOCA_LOG_ERR("Failed to create epoll instance: %s", strerror(errno));
        cleanup_objects(objs);
        return;
    }

    ev.events = EPOLLIN;
    ev.data.fd = ctrl_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, ctrl_fd, &ev) != 0) {
        DOCA_LOG_ERR("Failed to register control PE fd with epoll: %s", strerror(errno));
        goto fail;
    }
    ev.events = EPOLLIN;
    ev.data.fd = data_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, data_fd, &ev) != 0) {
        DOCA_LOG_ERR("Failed to register consumer PE fd with epoll: %s", strerror(errno));
        goto fail;
    }

    while (true) {
        int drained;

        /* Arm first so events pending now (or arriving during the drain below)
         * signal the fds; then process everything currently ready without
         * blocking. This closes the race where a setup step's internal
         * progress already consumed the awaited event (no new fd edge). */
        result = dmesh_doca_ctrl_arm(objs);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to arm control PE: %s", doca_error_get_descr(result));
            goto fail;
        }
        result = doca_pe_request_notification(objs->consumer_pe);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to arm consumer PE: %s", doca_error_get_descr(result));
            goto fail;
        }

        result = dmesh_doca_ctrl_drain(objs);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to drain control PE: %s", doca_error_get_descr(result));
            goto fail;
        }

        /* Bounded data-path drain: under sustained load the consumer PE always
         * has more work, so an unbounded drain-to-zero would starve the
         * throughput report and the control path. */
        for (drained = 0; drained < DATA_DRAIN_BUDGET; drained++) {
            if (doca_pe_progress(objs->consumer_pe) == 0)
                break;
        }

        result = dmesh_doca_ctrl_advance(objs, &state);
        if (result != DOCA_SUCCESS || state == DMESH_DOCA_STATE_ERROR) {
            DOCA_LOG_ERR("Control-path advance failed: %s", doca_error_get_descr(result));
            goto fail;
        }

        /* Budget exhausted means more data-path work is pending: loop again
         * without sleeping (the control path still ran above). */
        if (drained >= DATA_DRAIN_BUDGET)
            continue;

        /* Idle: sleep until either PE signals the next event. */
        if (epoll_wait(epfd, &ev, 1, -1) < 0) {
            if (errno == EINTR)
                continue;
            DOCA_LOG_ERR("epoll_wait failed: %s", strerror(errno));
            goto fail;
        }

        result = dmesh_doca_ctrl_clear_and_drain(objs, ctrl_fd);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to clear/drain control PE: %s", doca_error_get_descr(result));
            goto fail;
        }
        result = doca_pe_clear_notification(objs->consumer_pe,
                                            (doca_notification_handle_t)data_fd);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to clear consumer PE notification: %s", doca_error_get_descr(result));
            goto fail;
        }
        /* Work is processed by the bounded drain at the top of the next
         * iteration (arming in PROGRESS_ALL mode clears prior notifications). */
    }

fail:
    close(epfd);
    cleanup_objects(objs);
}

struct dpu_worker_thread_ctx {
    const struct global_config *gcfg;
    struct objects *objs;   /* allocated by the spawner; read by the reporter */
    int idx;
};

/*
 * One DPU worker thread: fully shared-nothing. Opens its own device and
 * representor handles, runs its own comch server ("DPUMesh<idx>"), owns its
 * own control/consumer PEs, DPA instance + thread pool and connection slots.
 * No DOCA object is shared across worker threads, so no locking is needed.
 */
static void *
dpu_worker_thread(void *arg)
{
    struct dpu_worker_thread_ctx *ctx = arg;
    struct objects *objs = ctx->objs;
    char server_name[32];
    doca_error_t result;

    objs->worker_idx = ctx->idx;

    result = open_doca_device_with_pci(ctx->gcfg->dev_pci_addr, NULL, &objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("worker %d: failed to open DOCA device: %s",
                     ctx->idx, doca_error_get_descr(result));
        return NULL;
    }

    result = open_doca_device_rep_with_pci(objs->dev,
                                           DOCA_DEVINFO_REP_FILTER_NET,
                                           ctx->gcfg->dev_rep_pci_addr,
                                           &objs->rep_dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("worker %d: failed to open DOCA device representor: %s",
                     ctx->idx, doca_error_get_descr(result));
        doca_dev_close(objs->dev);
        return NULL;
    }

    snprintf(server_name, sizeof(server_name), "DPUMesh%d", ctx->idx);

    if (getenv("DPUMESH_BUSY_POLL") != NULL)
        run_dpu_worker(objs, server_name);
    else
        run_dpu_worker_event_driven(objs, server_name);

    /* workers only return on error */
    DOCA_LOG_ERR("worker %d: exited", ctx->idx);
    return NULL;
}

void
run_dpu_workers(const struct global_config *gcfg)
{
    pthread_t *threads;
    struct dpu_worker_thread_ctx *ctxs;
    int num_threads = gcfg->num_threads > 0 ? gcfg->num_threads : 1;
    long prev_sent = 0, prev_recv = 0;
    struct timespec last, now;
    int i, started;

    DOCA_LOG_INFO("Starting %d DPU worker thread(s); servers DPUMesh0..DPUMesh%d, "
                  "%d connections and %d DPA threads each",
                  num_threads, num_threads - 1,
                  DMESH_MAX_CONNECTIONS, DPA_THREAD_POOL_SIZE);

    threads = calloc(num_threads, sizeof(*threads));
    ctxs = calloc(num_threads, sizeof(*ctxs));
    if (threads == NULL || ctxs == NULL) {
        DOCA_LOG_ERR("Failed to allocate DPU worker thread state");
        free(threads);
        free(ctxs);
        return;
    }

    for (i = 0; i < num_threads; i++) {
        ctxs[i].objs = calloc(1, sizeof(*ctxs[i].objs));
        if (ctxs[i].objs == NULL) {
            DOCA_LOG_ERR("Failed to allocate worker %d objects", i);
            break;
        }
        ctxs[i].gcfg = gcfg;
        ctxs[i].idx = i;
        if (pthread_create(&threads[i], NULL, dpu_worker_thread, &ctxs[i]) != 0) {
            DOCA_LOG_ERR("Failed to create DPU worker thread %d", i);
            break;
        }
    }
    started = i;

    /* Stats reporter: workers only ever increment their own (monotonic)
     * counters; this thread reads them all once a second and reports the
     * aggregate delta. Torn/late reads only skew stats momentarily, so no
     * synchronization is needed and the data path stays shared-nothing. */
    clock_gettime(CLOCK_MONOTONIC, &last);
    while (started > 0) {
        struct timespec ts = {.tv_sec = 1, .tv_nsec = 0};
        long sent = 0, recv = 0, dropped = 0;
        int pending = 0;
        double elapsed;
        int w, c;

        nanosleep(&ts, NULL);

        for (w = 0; w < started; w++) {
            struct objects *objs = ctxs[w].objs;

            sent += objs->sent_msg_cnt;
            recv += objs->recv_msg_cnt;
            for (c = 0; c < DMESH_MAX_CONNECTIONS; c++) {
                pending += objs->conns[c].dma_pending_cnt;
                dropped += objs->conns[c].dma_dropped_copies;
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        last = now;

        if (sent != prev_sent || recv != prev_recv)
            DOCA_LOG_INFO("TOTAL(%d workers): elapsed: %.2f, sent: %.0f/s, recv: %.0f/s, "
                          "dma_pending: %d, dma_dropped: %ld",
                          started, elapsed,
                          (sent - prev_sent) / elapsed,
                          (recv - prev_recv) / elapsed,
                          pending, dropped);
        prev_sent = sent;
        prev_recv = recv;
    }

    free(threads);
    free(ctxs);
}
