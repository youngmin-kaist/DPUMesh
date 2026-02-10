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

#include <time.h>

DOCA_LOG_REGISTER(DPU_WORKER);

#define BUFFER_SIZE 1024 * 1024

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

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                           &objs->dma_buffer, BUFFER_SIZE,
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    /* run DPA thread */
    result = dmesh_doca_run_dpa_thread(objs, objs->dpa_thread, objs->dpa_comch);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    // /* export mmap to DPU */
    // result = export_mmap_to_remote(objs, objs->local_mmap, objs->dma_buffer, 1024 * 1024, DPU_TO_HOST);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
    //     goto argp_cleanup;
    // }

    result = send_dma_request_to_dpa(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* poll PE */
    clock_gettime(CLOCK_MONOTONIC, &last);
    int cnt = 0;
    while (true) {
        doca_pe_progress(objs->consumer_pe);
        // if (doca_pe_progress(objs->consumer_pe) == 0)
        //     nanosleep(&ts, &ts);
        
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