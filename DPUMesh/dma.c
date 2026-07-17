#include "dma.h"

#include <stdlib.h>

#include <doca_log.h>
#include <doca_buf.h>
#include <doca_dma.h>
#include <doca_mmap.h>
#include <doca_error.h>
#include <errno.h>
#include <arpa/inet.h>

#include "dpa_common.h"
#include "object.h"
#include "common.h"
#include "dpa.h"
#include "buffer.h"

DOCA_LOG_REGISTER(DMA);

doca_error_t
init_dma_resources(struct objects *objs)
{
    doca_error_t result;

    result = alloc_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                                   &objs->dma_buffer, 1024 * 1024, 
                                   DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DMA mmap and buffer - %s",
                doca_error_get_name(result));
        return result;
    }

    /* wait for remote mmap info from the host */
    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    DOCA_LOG_INFO("Remote mmap is ready for DMA operations");
    return DOCA_SUCCESS;    
}

doca_error_t
send_dma_request_to_dpa(struct dmesh_conn *conn)
{
#ifndef DOCA_ARCH_DPU
    (void)conn;
    DOCA_LOG_ERR("Sending a DMA request to DPA is only supported on DPU");
    return DOCA_ERROR_NOT_SUPPORTED;
#else
    struct objects *objs = conn->objs;
    doca_error_t result;
    doca_dpa_dev_mmap_t src_mmap, dst_mmap;
    struct comch_dma_req_msg dma_req_msg;

    if (conn->sndbuf.mmap == NULL || conn->rcvbuf.mmap == NULL) {
        DOCA_LOG_ERR("Remote buffers are not ready for this connection");
        return DOCA_ERROR_BAD_STATE;
    }

    result = doca_mmap_dev_get_dpa_handle(conn->sndbuf.mmap, objs->dev, &src_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get local mmap DPA handle: %s",
                     doca_error_get_descr(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dst_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get remote mmap DPA handle: %s",
                     doca_error_get_descr(result));
        return result;
    }

    dma_req_msg.type = COMCH_MSG_TYPE_DMA_REQ;
    dma_req_msg.dpa_producer = objs->remote_dpa_producer;
    dma_req_msg.dpa_producer_comp = objs->remote_dpa_producer_comp;
    dma_req_msg.src_mmap = src_mmap;
    dma_req_msg.dst_mmap = dst_mmap;
    dma_req_msg.src_addr = (uint64_t)conn->sndbuf.buf;
    dma_req_msg.dst_addr = (uint64_t)conn->dma_buffer;
    dma_req_msg.length = 1024;
    DOCA_LOG_INFO("Sending DMA request to DPA: producer: 0x%lx, src_mmap=%u, dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u",
                    dma_req_msg.dpa_producer,          
                    dma_req_msg.src_mmap,
                  dma_req_msg.dst_mmap,
                  dma_req_msg.src_addr,
                  dma_req_msg.dst_addr,
                  dma_req_msg.length);

    result = dmesh_doca_dpa_msgq_send(&conn->dpa_comch->send,
                              &dma_req_msg,
                              sizeof(dma_req_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send DMA request to DPA: %s",
                     doca_error_get_descr(result));
        return result;
    }

    DOCA_LOG_INFO("DMA request sent to DPA successfully");
    return DOCA_SUCCESS;
#endif
}

doca_error_t
dmesh_dma_copy_to_rcvbuf(struct dmesh_conn *conn, uint32_t pos, uint32_t length)
{
    struct doca_buf *sbuf = NULL, *dbuf = NULL;
    doca_error_t result;

    if (get_num_free_dma_tasks(conn) == 0)
        return DOCA_ERROR_AGAIN;

    /* For src doca buffer, data len must be specified */
    result = doca_buf_inventory_buf_get_by_data(conn->buf_inv,
                                                conn->local_mmap,
                                                conn->dma_buffer + pos,
                                                length,
                                                &sbuf);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get src buffer from inventory: %s", doca_error_get_descr(result));
        return result;
    }

    result = doca_buf_inventory_buf_get_by_addr(conn->buf_inv,
                                                conn->rcvbuf.mmap,
                                                conn->rcvbuf.buf,
                                                length,
                                                &dbuf);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get dst buffer from inventory: %s", doca_error_get_descr(result));
        (void)doca_buf_dec_refcount(sbuf, NULL);
        return result;
    }

    result = submit_dma_task(conn, sbuf, dbuf);
    if (result == DOCA_ERROR_AGAIN) {
        /* bufs were never attached to a task; release them before retrying */
        (void)doca_buf_dec_refcount(dbuf, NULL);
        (void)doca_buf_dec_refcount(sbuf, NULL);
        return DOCA_ERROR_AGAIN;
    }
    /* on other failures submit_dma_task already released the bufs via
     * put_free_dma_task */
    return result;
}

void
dmesh_dma_defer_copy(struct dmesh_conn *conn, uint32_t pos, uint32_t length)
{
    int tail;

    if (conn->dma_pending == NULL || conn->dma_pending_cnt >= DMA_PENDING_MAX) {
        /* Sustained overload: inflow exceeds this connection's DMA throughput.
         * Counted silently; the throughput report surfaces pending/dropped. */
        conn->dma_dropped_copies++;
        return;
    }

    tail = (conn->dma_pending_head + conn->dma_pending_cnt) % DMA_PENDING_MAX;
    conn->dma_pending[tail].pos = pos;
    conn->dma_pending[tail].length = length;
    conn->dma_pending_cnt++;
}

void
dmesh_dma_pending_drain(struct dmesh_conn *conn)
{
    struct dma_pending_copy *p;
    doca_error_t result;

    if (conn->state != DMESH_CONN_RUNNING) {
        /* connection went away; discard its deferred copies */
        conn->dma_pending_cnt = 0;
        return;
    }

    while (conn->dma_pending_cnt > 0) {
        p = &conn->dma_pending[conn->dma_pending_head];

        result = dmesh_dma_copy_to_rcvbuf(conn, p->pos, p->length);
        if (result == DOCA_ERROR_AGAIN)
            break; /* still no capacity; retry on the next completion */

        /* submitted (or non-retryable failure: drop) */
        conn->dma_pending_head = (conn->dma_pending_head + 1) % DMA_PENDING_MAX;
        conn->dma_pending_cnt--;
    }
}

/*
 * DMA Memcpy task completed callback
 *
 * @dma_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dmesh_doca_dpa_dma_task_completed_cb(struct doca_dma_task_memcpy *dma_task,
					  union doca_data task_user_data,
					  union doca_data ctx_user_data)
{
    struct dmesh_conn *conn = (struct dmesh_conn *)ctx_user_data.ptr;
    struct dma_task_entry *entry = (struct dma_task_entry *)task_user_data.ptr;
    doca_error_t result;

    entry->in_flight = false;
    entry->result = DOCA_SUCCESS;
    result = put_free_dma_task(conn, dma_task);
    if (result != DOCA_SUCCESS)
        DOCA_LOG_ERR("Failed to return completed DMA task to queue: %s",
                     doca_error_get_descr(result));

    /* a task (and its bufs) just freed up: run this connection's deferred
     * copies. Task submission is allowed inside a completion callback. */
    dmesh_dma_pending_drain(conn);
}

/*
 * Memcpy task error callback
 *
 * @dma_task [in]: failed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dmesh_doca_dpa_dma_task_error_cb(struct doca_dma_task_memcpy *dma_task,
				      union doca_data task_user_data,
				      union doca_data ctx_user_data)
{
    struct dmesh_conn *conn = (struct dmesh_conn *)ctx_user_data.ptr;
	struct doca_task *task = doca_dma_task_memcpy_as_task(dma_task);
    struct dma_task_entry *entry = (struct dma_task_entry *)task_user_data.ptr;
    doca_error_t result;

    entry->in_flight = false;
    entry->result = doca_task_get_status(task);
    result = put_free_dma_task(conn, dma_task);
    if (result != DOCA_SUCCESS)
        DOCA_LOG_ERR("Failed to return errored DMA task to queue: %s",
                     doca_error_get_descr(result));

    DOCA_LOG_ERR("DMA task failed: %s", doca_error_get_descr(entry->result));

    int i;
    for (i = 0; i < conn->num_dma_tasks; i++) {
        entry = &conn->dma_task_entries[i];
        doca_task_free(doca_dma_task_memcpy_as_task(entry->task));
    }
}

/**
 * Callback triggered whenever DMA context state changes
 *
 * @user_data [in]: User data associated with the DMA context. Will hold struct dma_resources *
 * @ctx [in]: The DMA context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void dma_state_changed_cb(const union doca_data user_data,
				       struct doca_ctx *ctx,
				       enum doca_ctx_states prev_state,
				       enum doca_ctx_states next_state)
{
	(void)user_data;
	(void)ctx;
	(void)prev_state;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("DMA context has been stopped");
		/* We can stop progressing the PE */
		break;
	case DOCA_CTX_STATE_STARTING:
		/**
		 * The context is in starting state, this is unexpected for DMA.
		 */
		DOCA_LOG_ERR("DMA context entered into starting state. Unexpected transition");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("DMA context is running");
		break;
	case DOCA_CTX_STATE_STOPPING:
		/**
		 * doca_ctx_stop() has been called.
		 * In this sample, this happens either due to a failure encountered, in which case doca_pe_progress()
		 * will cause any inflight task to be flushed, or due to the successful compilation of the sample flow.
		 * In both cases, in this sample, doca_pe_progress() will eventually transition the context to idle
		 * state.
		 */
		DOCA_LOG_INFO("DMA context entered into stopping state. Any inflight tasks will be flushed");
		break;
	default:
		break;
	}
}

doca_error_t
init_dma_tasks(struct dmesh_conn *conn, int num_tasks)
{
    struct objects *objs = conn->objs;
    uint32_t max_buf_list_len, max_tasks;
    union doca_data ctx_user_data = {0};
    struct doca_ctx *dma_ctx;
    doca_error_t result;
    int i = 0;
    uint64_t max_buf_size;

    if (num_tasks <= 0) {
        DOCA_LOG_ERR("Requested number of DMA tasks must be greater than zero");
        return DOCA_ERROR_INVALID_VALUE;
    }

    /* Each in-flight DMA task holds two bufs (src + dst); size the inventory so
     * the free-task queue, not the inventory, is what limits in-flight work. */
    result = doca_buf_inventory_create(2 * (size_t)num_tasks + 16, &conn->buf_inv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create buffer inventory: %s", doca_error_get_descr(result));
        return result;
    }

    /* Start the buffer inventory */
    result = doca_buf_inventory_start(conn->buf_inv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start buffer inventory: %s", doca_error_get_descr(result));
        return result;
    }

    /* Get the maximum buffer list length for DMA memcpy tasks */
    result = doca_dma_cap_task_memcpy_get_max_buf_list_len(doca_dev_as_devinfo(objs->dev), &max_buf_list_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max buffer list length for DMA memcpy task: %s",
                     doca_error_get_descr(result));
        return result;
    }

    result = doca_dma_cap_task_memcpy_get_max_buf_size(doca_dev_as_devinfo(objs->dev), &max_buf_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max buffer size for DMA memcpy task: %s",
                     doca_error_get_descr(result));
        return result;
    }
    DOCA_LOG_INFO("DOCA DMA memcpy task max buffer list length: %u, max buffer size: %lu", max_buf_list_len, (unsigned long)max_buf_size);

    /* Create the DOCA DMA context */
    result = doca_dma_create(objs->dev, &conn->dma_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DOCA DMA context: %s",
                     doca_error_get_descr(result));
        return result;
    }

    /* Get the maximum number of DMA tasks */
    result = doca_dma_cap_get_max_num_tasks(conn->dma_ctx, &max_tasks);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max number of DMA tasks: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }
    if (num_tasks > (int)max_tasks) {
        DOCA_LOG_ERR("Requested number of DMA tasks [%d] exceed the limitation [%u]",
                     num_tasks, max_tasks);
        result = DOCA_ERROR_INVALID_VALUE;
        goto destroy_dma;
    }
    if (num_tasks <= 0) {
        DOCA_LOG_ERR("Requested number of DMA tasks must be greater than zero");
        result = DOCA_ERROR_INVALID_VALUE;
        goto destroy_dma;
    }
    conn->num_dma_tasks = num_tasks;
    DOCA_LOG_INFO("DOCA DMA max number of tasks: %u", max_tasks);

    dma_ctx = doca_dma_as_ctx(conn->dma_ctx);

    /* Set the state changed callback for the DMA context */
    result = doca_ctx_set_state_changed_cb(dma_ctx, dma_state_changed_cb);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DMA context state changed callback: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Set callback functions for DMA completion/error */
    result = doca_dma_task_memcpy_set_conf(conn->dma_ctx,
                                           dmesh_doca_dpa_dma_task_completed_cb,
                                           dmesh_doca_dpa_dma_task_error_cb,
                                           num_tasks);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DMA memcpy task configuration: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Set the user data for the DMA context */
    ctx_user_data.ptr = conn;
    doca_ctx_set_user_data(dma_ctx, ctx_user_data);

    /* DMA completions are data-path work: connect to the consumer PE, which the
     * steady-state datapath loop progresses. The control PE is not progressed
     * after init in the event-driven worker, so completions (and the buf
     * inventory returns they trigger) would never run there. */
    result = doca_pe_connect_ctx(objs->consumer_pe, dma_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to connect PE to DMA context: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Start the DMA context */
    result = doca_ctx_start(dma_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DMA context: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Deferred-copy ring for this connection */
    conn->dma_pending = calloc(DMA_PENDING_MAX, sizeof(*conn->dma_pending));
    if (conn->dma_pending == NULL) {
        result = DOCA_ERROR_NO_MEMORY;
        goto destroy_dma;
    }
    conn->dma_pending_head = 0;
    conn->dma_pending_cnt = 0;

    /* Allocate DMA tasks and put all of them in the free-task queue. */
    conn->dma_task_entries = calloc(num_tasks, sizeof(*conn->dma_task_entries));
    if (conn->dma_task_entries == NULL) {
        result = DOCA_ERROR_NO_MEMORY;
        goto destroy_dma;
    }

    TAILQ_INIT(&conn->free_dma_tasks);
    TAILQ_INIT(&conn->submission_dma_tasks);
    conn->num_free_dma_tasks = 0;
    conn->num_submission_dma_tasks = 0;
    for (i = 0; i < num_tasks; ++i) {
        struct dma_task_entry *entry = &conn->dma_task_entries[i];

        result = doca_dma_task_memcpy_alloc_init(conn->dma_ctx,
                                     NULL, /* src buf will be set when submitting the task */
                                     NULL, /* dst buf will be set when submitting the task */
                                     (union doca_data){.ptr = entry},
                                     &entry->task);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to allocate and initialize DMA memcpy task: %s",
                         doca_error_get_descr(result));
            goto free_tasks;
        }

        entry->owner = conn;
        entry->result = DOCA_SUCCESS;
        result = put_free_dma_task(conn, entry->task);
        if (result != DOCA_SUCCESS) {
            doca_task_free(doca_dma_task_memcpy_as_task(entry->task));
            entry->task = NULL;
            goto free_tasks;
        }
    }

    return DOCA_SUCCESS;

free_tasks:
    while (i-- > 0)
        doca_task_free(doca_dma_task_memcpy_as_task(conn->dma_task_entries[i].task));
    free(conn->dma_task_entries);
    conn->dma_task_entries = NULL;
    conn->num_dma_tasks = 0;
    conn->num_free_dma_tasks = 0;
    conn->num_submission_dma_tasks = 0;
destroy_dma:
    cleanup_dma_tasks(conn);
    return result;
}

doca_error_t
submit_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst)
{
    struct doca_dma_task_memcpy *dma_task;
    struct dma_task_entry *entry;
    doca_error_t result;

    if (conn == NULL || src == NULL || dst == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    dma_task = get_free_dma_task(conn);
    if (dma_task == NULL)
        return DOCA_ERROR_AGAIN;

    entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;

    doca_dma_task_memcpy_set_src(dma_task, src);
    doca_dma_task_memcpy_set_dst(dma_task, dst); 
    entry->result = DOCA_ERROR_IN_PROGRESS;

    result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_task));
    if (result == DOCA_SUCCESS) {
        entry->in_flight = true;
    } else {
        entry->result = result;
        if (put_free_dma_task(conn, dma_task) != DOCA_SUCCESS)
            DOCA_LOG_ERR("Failed to return unsubmitted DMA task to queue");
    }

    return result;
}

doca_error_t
enqueue_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst)
{
    struct doca_dma_task_memcpy *dma_task;
    doca_error_t result;

    if (conn == NULL || src == NULL || dst == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    dma_task = get_free_dma_task(conn);
    if (dma_task == NULL)
        return DOCA_ERROR_AGAIN;

    doca_dma_task_memcpy_set_src(dma_task, src);
    doca_dma_task_memcpy_set_dst(dma_task, dst);

    result = put_submission_dma_task(conn, dma_task);
    if (result != DOCA_SUCCESS && put_free_dma_task(conn, dma_task) != DOCA_SUCCESS)
        DOCA_LOG_ERR("Failed to return DMA task after enqueue failure");

    return result;
}

doca_error_t
progress_dma_submission_queue(struct dmesh_conn *conn, int max_tasks, int *num_submitted)
{
    struct doca_dma_task_memcpy *dma_task;
    struct dma_task_entry *entry;
    doca_error_t result;
    int submitted = 0;

    if (num_submitted != NULL)
        *num_submitted = 0;

    if (conn == NULL || max_tasks < 0)
        return DOCA_ERROR_INVALID_VALUE;

    while (max_tasks == 0 || submitted < max_tasks) {
        dma_task = get_submission_dma_task(conn);
        if (dma_task == NULL)
            break;

        entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;
        entry->result = DOCA_ERROR_IN_PROGRESS;
        result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_task));
        if (result == DOCA_SUCCESS) {
            entry->in_flight = true;
            submitted++;
            continue;
        }

        entry->result = result;
        if (result == DOCA_ERROR_AGAIN) {
            TAILQ_INSERT_HEAD(&conn->submission_dma_tasks, entry, entries);
            entry->in_submission_queue = true;
            conn->num_submission_dma_tasks++;
        } else if (put_free_dma_task(conn, dma_task) != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to return rejected DMA task to free queue");
        }

        if (num_submitted != NULL)
            *num_submitted = submitted;
        return result;
    }

    if (num_submitted != NULL)
        *num_submitted = submitted;
    return DOCA_SUCCESS;
}

struct doca_dma_task_memcpy *
get_free_dma_task(struct dmesh_conn *conn)
{
    struct dma_task_entry *entry;

    if (conn == NULL)
        return NULL;

    entry = TAILQ_FIRST(&conn->free_dma_tasks);
    if (entry == NULL)
        return NULL;

    TAILQ_REMOVE(&conn->free_dma_tasks, entry, entries);
    entry->in_free_queue = false;
    conn->num_free_dma_tasks--;

    return entry->task;
}

doca_error_t
put_free_dma_task(struct dmesh_conn *conn, struct doca_dma_task_memcpy *dma_task)
{
    struct dma_task_entry *entry;
    const struct doca_buf *src;
    struct doca_buf *dst;

    if (conn == NULL || dma_task == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;
    if (entry == NULL || entry->owner != conn || entry->task != dma_task ||
        entry->in_free_queue || entry->in_submission_queue || entry->in_flight)
        return DOCA_ERROR_INVALID_VALUE;

    src = doca_dma_task_memcpy_get_src(dma_task);
    dst = doca_dma_task_memcpy_get_dst(dma_task);

    doca_dma_task_memcpy_set_src(dma_task, NULL);
    doca_dma_task_memcpy_set_dst(dma_task, NULL);
    if (src != NULL)
        (void)doca_buf_dec_refcount((struct doca_buf *)src, NULL);
    if (dst != NULL)
        (void)doca_buf_dec_refcount(dst, NULL);

    TAILQ_INSERT_TAIL(&conn->free_dma_tasks, entry, entries);
    entry->in_free_queue = true;
    conn->num_free_dma_tasks++;

    return DOCA_SUCCESS;
}

struct doca_dma_task_memcpy *
get_submission_dma_task(struct dmesh_conn *conn)
{
    struct dma_task_entry *entry;

    if (conn == NULL)
        return NULL;

    entry = TAILQ_FIRST(&conn->submission_dma_tasks);
    if (entry == NULL)
        return NULL;

    TAILQ_REMOVE(&conn->submission_dma_tasks, entry, entries);
    entry->in_submission_queue = false;
    conn->num_submission_dma_tasks--;

    return entry->task;
}

doca_error_t
put_submission_dma_task(struct dmesh_conn *conn, struct doca_dma_task_memcpy *dma_task)
{
    struct dma_task_entry *entry;

    if (conn == NULL || dma_task == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;
    if (entry == NULL || entry->owner != conn || entry->task != dma_task ||
        entry->in_free_queue || entry->in_submission_queue || entry->in_flight ||
        doca_dma_task_memcpy_get_src(dma_task) == NULL ||
        doca_dma_task_memcpy_get_dst(dma_task) == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    TAILQ_INSERT_TAIL(&conn->submission_dma_tasks, entry, entries);
    entry->in_submission_queue = true;
    conn->num_submission_dma_tasks++;

    return DOCA_SUCCESS;
}

int
get_num_free_dma_tasks(const struct dmesh_conn *conn)
{
    return conn == NULL ? 0 : conn->num_free_dma_tasks;
}

int
get_num_submission_dma_tasks(const struct dmesh_conn *conn)
{
    return conn == NULL ? 0 : conn->num_submission_dma_tasks;
}

void
cleanup_dma_tasks(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    struct doca_ctx *dma_ctx;
    enum doca_ctx_states state;
    doca_error_t result;
    int i;

    if (conn == NULL || conn->dma_ctx == NULL)
        return;

    dma_ctx = doca_dma_as_ctx(conn->dma_ctx);
    if (doca_ctx_get_state(dma_ctx, &state) == DOCA_SUCCESS &&
        state != DOCA_CTX_STATE_IDLE) {
        result = doca_ctx_stop(dma_ctx);
        if (result == DOCA_SUCCESS) {
            while (doca_ctx_get_state(dma_ctx, &state) == DOCA_SUCCESS &&
                   state != DOCA_CTX_STATE_IDLE)
                doca_pe_progress(objs->consumer_pe); /* DMA ctx lives on the consumer PE */
        } else {
            DOCA_LOG_ERR("Failed to stop DMA context: %s", doca_error_get_descr(result));
        }
    }

    if (conn->dma_task_entries != NULL) {
        for (i = 0; i < conn->num_dma_tasks; ++i) {
            if (conn->dma_task_entries[i].task != NULL)
                doca_task_free(doca_dma_task_memcpy_as_task(conn->dma_task_entries[i].task));
        }
    }

    free(conn->dma_task_entries);
    conn->dma_task_entries = NULL;
    conn->num_dma_tasks = 0;
    conn->num_free_dma_tasks = 0;
    conn->num_submission_dma_tasks = 0;
    TAILQ_INIT(&conn->free_dma_tasks);
    TAILQ_INIT(&conn->submission_dma_tasks);

    free(conn->dma_pending);
    conn->dma_pending = NULL;
    conn->dma_pending_head = 0;
    conn->dma_pending_cnt = 0;

    doca_dma_destroy(conn->dma_ctx);
    conn->dma_ctx = NULL;
}
