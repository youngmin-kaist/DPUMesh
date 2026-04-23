#include "host_worker.h"
#include "object.h"
#include "comch_client.h"
#include "comch_producer.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "config.h"
#include "buffer.h"
#include "comch_msgq.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "ring.h"

#include <doca_log.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>

#define BUFFER_SIZE (1024 * 1024)

DOCA_LOG_REGISTER(HOST_WORKER);

void 
run_host_worker(struct objects *objs)
{
    doca_error_t result;
	// double elapsed = 0.0;

    DOCA_LOG_INFO("Starting Host worker");

    /* initialize DOCA Comch client objects */
    result = init_comch_ctrl_path_client("DPUMesh", objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path client: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }
    
    /* initialize DOCA Comch producer objects */
    result = init_comch_datapath_producer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send message over comch data path: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    /* setup DMA ring */
    result = setup_dma_ring(objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DMA ring: %s", doca_error_get_descr(result));
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

    /* export DMA buffer mmap to DPU */
    result = export_mmap_to_remote(objs, objs->local_mmap, 
                                   objs->dma_buffer, BUFFER_SIZE, 
                                   DMA_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }

    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init DPA objects: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DPA thread: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    result = init_comch_dpa_msgq(objs, objs->producer_pe);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch DPA datapath: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        goto argp_cleanup;
    }

    // result = setup_dpa_buf_array(objs, DMA_RING_SIZE, objs->local_mmap);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to setup DPA buffer array: %s", doca_error_get_descr(result));
    //     goto argp_cleanup;
    // }

    // result = dmesh_doca_run_dpa_thread(objs, objs->dpa_thread, objs->dpa_comch);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = send_dma_request_to_dpa(objs);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp _cleanup;
    // }

    int pos = 0;
    uint64_t producer_seq = 0;
    struct dma_desc *desc;
    doca_dpa_dev_mmap_t local_mmap;
    doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &local_mmap);
    const int msg_size = 8192;
    int aligned_msg_size = (msg_size + (DMA_ADDR_ALIGN - 1)) & ~(DMA_ADDR_ALIGN - 1);

    DOCA_LOG_INFO("consumer state magic: 0x%lx\n", objs->dma_ring->consumer_state->consumer_seq);

    while (true) {
        int pe_progress;
        while ((pe_progress = doca_pe_progress(objs->pe)) > 0) {
            /* drain completions / keep host side progressing */
        }

        while (!dma_ring_has_free_slot(objs->dma_ring)) {
            dma_ring_refresh_consumer(objs->dma_ring);
            // DOCA_LOG_INFO("No free slot in DMA ring, observed consumer_seq: %lu, producer_seq: %lu\n",
                // objs->dma_ring->observed_consumer_seq, objs->dma_ring->producer_seq);
        }

        if (pos + aligned_msg_size > BUFFER_SIZE) {
            pos = 0;
        }

        producer_seq = objs->dma_ring->producer_seq;
        desc = get_dma_desc_for_seq(objs->dma_ring, producer_seq);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->dma_buffer + pos;
        desc->size = aligned_msg_size;

        /*
         * Host owns descriptor writes. seq is the only per-desc publish marker;
         * DPA consumes it with acquire ordering and never writes this cacheline.
         */
        // __atomic_store_n(&desc->seq, producer_seq + 1, __ATOMIC_RELEASE);
        desc->seq = producer_seq + 1;

        pos += aligned_msg_size;
        objs->dma_ring->producer_seq = producer_seq + 1;
    }

    DOCA_LOG_INFO("Finished Host worker");

argp_cleanup:
    clean_argp();
// exit: 
    return;
}
