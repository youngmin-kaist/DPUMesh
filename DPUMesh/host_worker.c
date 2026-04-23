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
#include <time.h>
#include <assert.h>

#define BUFFER_SIZE (1024 * 1024)

DOCA_LOG_REGISTER(HOST_WORKER);
void 
run_host_worker(struct objects *objs)
{
    doca_error_t result;
    struct timespec ts = {0, 1000};

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

    int idx = 0;
    int pos = 0;
    struct dma_desc *desc;
    doca_dpa_dev_mmap_t local_mmap;
    doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &local_mmap);
    const int msg_size = 8192;
    int aligned_msg_size = (msg_size + (DMA_ADDR_ALIGN - 1)) & ~(DMA_ADDR_ALIGN - 1);

    while (true) {
        // if (doca_pe_progress(objs->pe) == 0)
        //     nanosleep(&ts, &ts);
        while (doca_pe_progress(objs->pe) > 0);

        desc = get_next_dma_desc(objs->dma_ring);
        while (desc->valid)
            nanosleep(&ts, &ts);

        if (pos + aligned_msg_size > BUFFER_SIZE) {
            pos = 0;
        }

        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->dma_buffer + pos;
        desc->idx = idx++;
        desc->size = aligned_msg_size;
        desc->valid = 1;
        // DOCA_LOG_INFO("Produced DMA desc - idx: %lu, addr: %p, size: %zu, pos: %d", 
                    //   desc->idx, (void *)desc->addr, desc->size, pos);

        pos += aligned_msg_size;
    }

    DOCA_LOG_INFO("Finished Host worker");

argp_cleanup:
    clean_argp();
exit: 
    return;
}
