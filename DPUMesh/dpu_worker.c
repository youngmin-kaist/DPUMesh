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
#include <sys/eventfd.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>

DOCA_LOG_REGISTER(DPU_WORKER);

void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;
    struct timespec last, now = {0, 1000};
	double elapsed = 0.0;

    DOCA_LOG_INFO("Starting DPU worker");

    /* create a progress engine */
    result = doca_pe_create(&(objs->pe));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
        return;
    }

    /* initialize DOCA comch server */
    result = init_comch_ctrl_path_server("DPUMesh", objs, true);
    // result = init_comch_ctrl_path_server(objs->dev, objs->rep_dev, objs->pe, &objs->cc_server,
    //                                     "DPUMesh", (void *)objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path server: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }
    
    /* Wait for the DOCA comch client */
    while (objs->connection == NULL) {
        if (doca_pe_progress(objs->pe) == 0)
            nanosleep(&now, &now);
    }
    DOCA_LOG_INFO("Server connection established");

    /* initialize DOCA comch datapath consumer */
    result = init_comch_datapath_consumer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed in comch datapath recv msg: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* initialize DPA objects */
    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init DPA objects: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* create DPA thread */
    result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DPA thread: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* initialize comch DPA message queue */
    result = init_comch_dpa_msgq(objs, objs->consumer_pe);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch DPA datapath: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* wait for remote mmap from the host to be ready */
    while (objs->ring_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    /* setup DPA buffer array with remote mmap */
    result = setup_dpa_buf_array(objs, DMA_RING_SIZE, objs->ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DPA buffer array: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                           &objs->dma_buffer, BUFFER_SIZE,
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    /* initialize DMA tasks */
    result = init_dma_tasks(objs, 8192);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init DMA tasks: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* Wait for remote mmaps to be ready */
    while (objs->sndbuf.mmap == NULL || objs->rcvbuf.mmap == NULL) {
        doca_pe_progress(objs->pe);
    }
    DOCA_LOG_INFO("Remote mmaps for send and receive buffers are ready, %p, %p", objs->sndbuf.mmap, objs->rcvbuf.mmap);

    /* run DPA thread */
    result = dmesh_doca_run_dpa_thread(objs, objs->dpa_thread, objs->dpa_comch);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    result = send_dma_request_to_dpa(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* notification-driven event handlin for PE */
    doca_event_handle_t event_handle = doca_event_invalid_handle;
    struct epoll_event events_in = {.events = EPOLLIN, .data.fd = 0};

    /* create epoll fd */
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        DOCA_LOG_ERR("Failed to create epoll instance");
        return;
    }

    /* get notification handle for PE */
    result = doca_pe_get_notification_handle(objs->consumer_pe, &event_handle);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get PE notification handle for consumer PE: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }
    fprintf(stderr, "PE notification handle for consumer PE: %d\n", event_handle);

    /* add epoll event */
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_handle, &events_in) != 0) {
        DOCA_LOG_ERR("Failed to add event handle to epoll");
        return;
    }

    /* get notification handle for PE */
    result = doca_pe_get_notification_handle(objs->pe, &event_handle);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get PE notification handle for consumer PE: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }
    fprintf(stderr, "PE notification handle for PE: %d\n", event_handle);

    /* add epoll event */
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_handle, &events_in) != 0) {
        DOCA_LOG_ERR("Failed to add event handle to epoll");
        return;
    }

    doca_pe_request_notification(objs->consumer_pe);
    doca_pe_request_notification(objs->pe);

    int ret, fd;
    clock_gettime(CLOCK_MONOTONIC, &last);
    while (1) {
        ret = epoll_wait(epoll_fd, &events_in, 1, -1);
        if (ret == -1) {
            if (errno == EINTR) {
                continue; // Interrupted by signal, retry
            }
            DOCA_LOG_ERR("Failed to wait on epoll: %s", strerror(errno));
            return;
        }
        fd = events_in.data.fd;

        fprintf(stderr, "Event received on PE notification handle, fd: %d\n", fd);
        doca_pe_clear_notification(objs->consumer_pe, 0);
        doca_pe_clear_notification(objs->pe, 0);

        while (doca_pe_progress(objs->consumer_pe) != 0) {
        }
        while (doca_pe_progress(objs->pe) != 0) {
        }

        doca_pe_request_notification(objs->consumer_pe);
        doca_pe_request_notification(objs->pe);

        clock_gettime(CLOCK_MONOTONIC, &now);
		// elapsed = diff_sec(&last, &now);
        elapsed = (now.tv_sec - last.tv_sec) + 
                  (now.tv_nsec - last.tv_nsec) / 1e9;
		if (elapsed >= 1.0) {
            if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0)
			    DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s", elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt);
			objs->sent_msg_cnt = 0;
			objs->recv_msg_cnt = 0;
			last = now;
		}
    }

    /* poll PE */
    // clock_gettime(CLOCK_MONOTONIC, &last);
    while (true) {
        doca_pe_progress(objs->pe);
        doca_pe_progress(objs->consumer_pe);
        
        // if (cnt++ % 10 == 0)
        //     doca_pe_progress(objs->pe);  

        // if(objs->dpa_thread->buf) {
        //     char temp[128];
        //     result = doca_dpa_d2h_memcpy(objs->dpa_thread->dpa, temp, objs->dpa_thread->buf, 128);
        //     if (result != DOCA_SUCCESS) {
        //         DOCA_LOG_ERR("Failed to copy data from DPA memory to host: %s",
        //             doca_error_get_descr(result));
        //         goto argp_cleanup;
        //     }
        //     DOCA_LOG_INFO("Copied data from DPA memory: %s", temp);
        // }

        clock_gettime(CLOCK_MONOTONIC, &now);
		// elapsed = diff_sec(&last, &now);
        elapsed = (now.tv_sec - last.tv_sec) + 
                  (now.tv_nsec - last.tv_nsec) / 1e9;
		if (elapsed >= 1.0) {
            if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0)
			    DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s", elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt);
			objs->sent_msg_cnt = 0;
			objs->recv_msg_cnt = 0;
			last = now;
		}
    }

argp_cleanup:
    clean_argp();
}

/*
 * Steady-state data path: busy-poll the consumer PE and report throughput once
 * per second. Extracted so the event-driven worker can reuse it. Does not return.
 */
void
dmesh_doca_run_datapath(struct objects *objs)
{
    struct timespec last, now;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &last);
    while (true) {
        doca_pe_progress(objs->consumer_pe);

        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0)
                DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s",
                              elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt);
            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;
        }
    }
}

/*
 * Event-driven (on-demand) DPU worker.
 *
 * Same initialization as run_dpu_worker(), but instead of busy-polling the
 * control PE while waiting for host-driven control messages (connection, ring
 * mmap, remote mmap), it registers the control PE notification fd with epoll and
 * only progresses the PE when the fd signals. The init sequencing lives in the
 * shared dmesh_doca_ctrl_advance() state machine (comch_server.c), so this loop
 * mirrors exactly what the Rust AsyncFd driver will do via FFI.
 */
void
run_dpu_worker_event_driven(struct objects *objs)
{
    doca_error_t result;
    enum dmesh_doca_init_state state, prev;
    int fd, epfd;
    struct epoll_event ev;

    DOCA_LOG_INFO("Starting DPU worker (event-driven)");

    /* Start the control-path server without blocking on the host connection. */
    result = start_comch_ctrl_path_server("DPUMesh", objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start comch control path server: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* Get the control PE notification fd and register it with epoll. */
    result = dmesh_doca_ctrl_get_fd(objs, &fd);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get control PE notification fd: %s", doca_error_get_descr(result));
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
    ev.data.fd = fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        DOCA_LOG_ERR("Failed to register control PE fd with epoll: %s", strerror(errno));
        close(epfd);
        cleanup_objects(objs);
        return;
    }

    state = objs->phase; /* DMESH_DOCA_STATE_SERVER_STARTED */
    while (state != DMESH_DOCA_STATE_RUNNING) {
        /* Arm first so events pending now (or arriving during the drain below)
         * signal the fd; then process everything currently ready and cascade
         * through as many phases as are unlocked, without blocking. This closes
         * the race where a phase's internal progress already consumed the
         * awaited control message (so no further fd edge would arrive). */
        result = dmesh_doca_ctrl_arm(objs);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to arm control PE: %s", doca_error_get_descr(result));
            goto fail;
        }

        do {
            prev = state;

            result = dmesh_doca_ctrl_drain(objs);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to drain control PE: %s", doca_error_get_descr(result));
                goto fail;
            }

            result = dmesh_doca_ctrl_advance(objs, &state);
            if (result != DOCA_SUCCESS || state == DMESH_DOCA_STATE_ERROR) {
                DOCA_LOG_ERR("Control-path init failed: %s", doca_error_get_descr(result));
                goto fail;
            }
        } while (state != prev && state != DMESH_DOCA_STATE_RUNNING);

        if (state == DMESH_DOCA_STATE_RUNNING)
            break;

        /* Nothing ready: block until the control PE fd signals the next event. */
        if (epoll_wait(epfd, &ev, 1, -1) < 0) {
            if (errno == EINTR)
                continue;
            DOCA_LOG_ERR("epoll_wait on control PE fd failed: %s", strerror(errno));
            goto fail;
        }

        result = dmesh_doca_ctrl_clear_and_drain(objs, fd);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to clear/drain control PE: %s", doca_error_get_descr(result));
            goto fail;
        }
        /* Phase transitions happen at the top of the next iteration (post re-arm). */
    }

    close(epfd);
    DOCA_LOG_INFO("DPU worker initialization complete; entering data path");

    dmesh_doca_run_datapath(objs);
    return;

fail:
    close(epfd);
    cleanup_objects(objs);
}