#include "object.h"
#include "comch_server.h"
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

DOCA_LOG_REGISTER(DPU_WORKER);

static void
report_throughput(struct objects *objs, struct timespec *last)
{
    struct timespec now;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &now);
    elapsed = (now.tv_sec - last->tv_sec) +
              (now.tv_nsec - last->tv_nsec) / 1e9;
    if (elapsed >= 1.0) {
        if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0)
            DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s",
                          elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt);
        objs->sent_msg_cnt = 0;
        objs->recv_msg_cnt = 0;
        *last = now;
    }
}

/*
 * Baseline (busy-poll) DPU worker.
 *
 * Multi-connection: the shared infrastructure (DPA instance + thread pool,
 * consumer PE, DMA engine) is built once up front; every new host connection
 * is then bound to a slot by the connection callback and driven through its
 * own state machine by dmesh_doca_ctrl_advance(). Both PEs are busy-polled.
 */
void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;
    enum dmesh_doca_init_state state;
    struct timespec last;

    DOCA_LOG_INFO("Starting DPU worker (busy-poll)");

    /* start the control-path server (non-blocking; creates objs->pe) */
    result = start_comch_ctrl_path_server("DPUMesh", objs, true);
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

    clock_gettime(CLOCK_MONOTONIC, &last);
    while (true) {
        doca_pe_progress(objs->pe);
        doca_pe_progress(objs->consumer_pe);

        result = dmesh_doca_ctrl_advance(objs, &state);
        if (result != DOCA_SUCCESS)
            DOCA_LOG_ERR("Control-path advance failed: %s", doca_error_get_descr(result));

        report_throughput(objs, &last);
    }

argp_cleanup:
    clean_argp();
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
run_dpu_worker_event_driven(struct objects *objs)
{
    doca_error_t result;
    enum dmesh_doca_init_state state;
    int ctrl_fd, data_fd, epfd;
    struct epoll_event ev;
    struct timespec last;

    DOCA_LOG_INFO("Starting DPU worker (event-driven)");

    /* Start the control-path server without blocking on the host connection. */
    result = start_comch_ctrl_path_server("DPUMesh", objs, true);
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

    clock_gettime(CLOCK_MONOTONIC, &last);
    while (true) {
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
        while (doca_pe_progress(objs->consumer_pe) != 0)
            ;

        result = dmesh_doca_ctrl_advance(objs, &state);
        if (result != DOCA_SUCCESS || state == DMESH_DOCA_STATE_ERROR) {
            DOCA_LOG_ERR("Control-path advance failed: %s", doca_error_get_descr(result));
            goto fail;
        }

        report_throughput(objs, &last);

        /* Sleep until either PE signals the next event. */
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
        while (doca_pe_progress(objs->consumer_pe) != 0)
            ;
        /* Connection state machines advance at the top of the next iteration. */
    }

fail:
    close(epfd);
    cleanup_objects(objs);
}
