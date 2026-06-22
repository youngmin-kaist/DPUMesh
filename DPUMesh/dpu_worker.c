#include "object.h"
#include "comch_server.h"
#include "dpa.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "dma.h"
#include "config.h"
#include "comch_msgq.h"
#include "ring.h"
#include "dpa_common.h"
#include "buffer.h"

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>

#include <time.h>

DOCA_LOG_REGISTER(DPU_WORKER);

#define DMESH_DPU_PENDING_SEND_DRAIN_BUDGET 64U
#define DMESH_DPU_PENDING_SEND_COLLECT_POLL_BUDGET 1024U
#define DMESH_DPU_PENDING_SEND_DRAIN_THRESHOLD 64U
#define DMESH_DPU_WARMUP_MAX_NS UINT64_C(1000000000)
#define DMESH_DPU_WARMUP_SAMPLE_NS UINT64_C(100000000)
#define DMESH_DPU_WARMUP_TARGET_SUBMIT_PER_LOOP_X100 500U
#define DMESH_DPU_WARMUP_MIN_SAMPLE_SUBMITTED 10000U

static uint64_t
dmesh_dpu_timespec_ns(const struct timespec *ts)
{
    return ((uint64_t)ts->tv_sec * UINT64_C(1000000000)) +
           (uint64_t)ts->tv_nsec;
}

static int
dmesh_dpu_collect_pending_sends(struct objects *objs,
                                uint32_t poll_budget,
                                uint32_t drain_threshold)
{
    int progressed = 0;

    if (objs == NULL || objs->consumer_pe == NULL)
        return 0;

    for (uint32_t i = 0; i < poll_budget; i++) {
        int current = doca_pe_progress(objs->consumer_pe);

        progressed += current;
        if (current == 0 || objs->dpa_comch->send.pending_count >= drain_threshold)
            break;
    }

    return progressed;
}

static doca_error_t
dmesh_dpu_progress_pending_pipeline(struct objects *objs,
                                    int *collect_progressed,
                                    uint32_t *pending_submitted)
{
    doca_error_t result;
    int local_progressed;
    uint32_t local_submitted = 0;

    local_progressed = dmesh_dpu_collect_pending_sends(objs,
                                                       DMESH_DPU_PENDING_SEND_COLLECT_POLL_BUDGET,
                                                       DMESH_DPU_PENDING_SEND_DRAIN_THRESHOLD);

    result = dmesh_doca_dpa_msgq_drain_pending(&objs->dpa_comch->send,
                                               DMESH_DPU_PENDING_SEND_DRAIN_BUDGET,
                                               &local_submitted);
    if (result != DOCA_SUCCESS &&
        result != DOCA_ERROR_AGAIN &&
        result != DOCA_ERROR_NO_MEMORY) {
        DOCA_LOG_ERR("Failed to drain pending DPA MsgQ sends: %s",
                     doca_error_get_name(result));
    }

    if (collect_progressed != NULL)
        *collect_progressed = local_progressed;
    if (pending_submitted != NULL)
        *pending_submitted = local_submitted;

    return result;
}

static void
dmesh_dpu_warmup_pending_pipeline(struct objects *objs)
{
    struct timespec start_ts, now_ts, ts = {0, 1000};
    uint64_t start_ns;
    uint64_t sample_ns;
    uint64_t now_ns;
    uint64_t total_submitted = 0;
    uint64_t sample_submitted = 0;
    uint32_t sample_loops = 0;
    uint32_t best_submit_per_loop_x100 = 0;

    clock_gettime(CLOCK_MONOTONIC, &start_ts);
    start_ns = dmesh_dpu_timespec_ns(&start_ts);
    sample_ns = start_ns;

    for (;;) {
        int collect_progressed = 0;
        uint32_t pending_submitted = 0;

        (void)dmesh_dpu_progress_pending_pipeline(objs,
                                                  &collect_progressed,
                                                  &pending_submitted);
        total_submitted += pending_submitted;
        sample_submitted += pending_submitted;
        sample_loops++;

        if (collect_progressed == 0 && pending_submitted == 0)
            nanosleep(&ts, &ts);

        if ((sample_loops & 1023U) != 0)
            continue;

        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        now_ns = dmesh_dpu_timespec_ns(&now_ts);

        if (now_ns - sample_ns >= DMESH_DPU_WARMUP_SAMPLE_NS) {
            uint32_t submit_per_loop_x100 =
                sample_loops == 0 ? 0 :
                (uint32_t)((sample_submitted * 100U) / sample_loops);

            if (submit_per_loop_x100 > best_submit_per_loop_x100)
                best_submit_per_loop_x100 = submit_per_loop_x100;

            if (sample_submitted >= DMESH_DPU_WARMUP_MIN_SAMPLE_SUBMITTED &&
                submit_per_loop_x100 >= DMESH_DPU_WARMUP_TARGET_SUBMIT_PER_LOOP_X100)
                break;

            sample_submitted = 0;
            sample_loops = 0;
            sample_ns = now_ns;
        }

        if (now_ns - start_ns >= DMESH_DPU_WARMUP_MAX_NS)
            break;
    }

    /*
     * This is a measurement warm-up: requests processed here intentionally
     * fill the DPA/DPU closed-loop pipeline but are not included in the
     * per-second DPU worker counters.
     */
    objs->sent_msg_cnt = 0;
    objs->recv_msg_cnt = 0;
    DOCA_LOG_INFO("DPU worker warmup complete: submitted=%lu best_submit_per_loop=%u.%02u",
                  total_submitted,
                  best_submit_per_loop_x100 / 100U,
                  best_submit_per_loop_x100 % 100U);
}

void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;
    struct timespec last, now, ts = {0, 1000};
	double elapsed = 0.0;

    DOCA_LOG_INFO("Starting DPU worker");

    /* initialize DOCA comch server */
    result = init_comch_ctrl_path_server("DPUMesh", objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path server: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

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

    while (objs->ring_consumer_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    result = setup_dpa_consumer_state_buf_array(objs, objs->ring_consumer_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DPA consumer state buffer array: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_hugepage_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                           &objs->dma_buffer, BUFFER_SIZE,
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    result = setup_dpa_host_mmap_buf_array(objs, objs->remote_mmap, objs->remote_buf_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DPA host mmap buffer array: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    result = setup_dpa_dpu_mmap_buf_array(objs, objs->local_mmap, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DPA DPU private mmap buffer array: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    /* run DPA thread */
    result = dmesh_doca_run_dpa_thread(objs, objs->dpa_thread, objs->dpa_comch);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    DOCA_LOG_INFO("Remote mmap is ready, DPU worker setup is complete\n");
    dmesh_dpu_warmup_pending_pipeline(objs);

    /* poll PE */
    int progress_pe = 0;
    int progress_pending_send = 0;
    int progress_pending_submitted = 0;
    int loop_cnt = 0;
    clock_gettime(CLOCK_MONOTONIC, &last);
#if DEBUG_LOG
    int sleep_cnt = 0;
#endif
    while (true) {
        int progressed = 0;
        uint32_t pending_submitted = 0;
        loop_cnt++;

        result = dmesh_dpu_progress_pending_pipeline(objs,
                                                     &progressed,
                                                     &pending_submitted);
        progress_pending_send += progressed;
        progressed += (int)pending_submitted;
        progress_pending_submitted += (int)pending_submitted;

        if (progressed == 0) {
            nanosleep(&ts, &ts);

#if DEBUG_LOG
//             sleep_cnt++;
//             if (sleep_cnt % 10000 == 0)
//                 DOCA_LOG_INFO("No progress made, sleeping for a bit...");
//         } else {
//             sleep_cnt = 0;
#endif
        }

        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - last.tv_sec) + 
                  (now.tv_nsec - last.tv_nsec) / 1e9;

		if (elapsed >= 1.0) {
            // if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0)
            DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s", elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt);
            DOCA_LOG_INFO("Loops: %d, PE: %d, pending send collect: %d, submitted pending sends: %d\n",
                            loop_cnt,
                            progress_pe,
                            progress_pending_send,
                            progress_pending_submitted);
            progress_pe = 0;
            progress_pending_send = 0;
            progress_pending_submitted = 0;
            loop_cnt = 0;

			objs->sent_msg_cnt = 0;
			objs->recv_msg_cnt = 0;
			last = now;
		}
    }

argp_cleanup:
    destroy_mmap_and_unmap_hugepage_buffer(objs->local_mmap, objs->dma_buffer, BUFFER_SIZE);
    objs->local_mmap = NULL;
    objs->dma_buffer = NULL;
    clean_argp();
}
