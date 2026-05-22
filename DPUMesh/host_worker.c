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
#include "grpc_offload.h"

#include <doca_log.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>

#include <stdio.h>

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

    // result = init_dpa_objects(objs);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to init DPA objects: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to create DPA thread: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = init_comch_dpa_msgq(objs, objs->producer_pe);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to init comch DPA datapath: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

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

    struct dmesh_grpc_arena app_arena = {0};
    uint32_t request_id = 1;

    DOCA_LOG_INFO("consumer state magic: 0x%lx\n", objs->dma_ring->consumer_state->consumer_seq);
    result = dmesh_grpc_arena_init(&app_arena, objs->dma_buffer, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to initialize gRPC arena: %s", doca_error_get_descr(result));
        goto argp_cleanup;
    }
    objs->grpc_offload = &app_arena;
    
    int pe_progress;
    while ((pe_progress = doca_pe_progress(objs->pe)) > 0) {
        /* drain completions / keep host side progressing */
        DOCA_LOG_INFO("Made progress on PE: %d\n", pe_progress);
    }

    while (true) {
        struct dmesh_grpc_hello_request *req = NULL;
        char name[64];
        uint32_t scores[4];
        int name_len;

        // while (!dma_ring_has_free_slot(objs->dma_ring)) {
        //     dma_ring_refresh_consumer(objs->dma_ring);
        //     // doca_pe_progress(objs->pe);
        // }

        while (doca_pe_progress(objs->pe)) {
            /* drain completions / keep host side progressing */
        }
        dma_ring_refresh_consumer(objs->dma_ring);
        dmesh_grpc_arena_reclaim_through(&app_arena,
                                            objs->dma_ring->observed_consumer_seq);

        name_len = snprintf(name, sizeof(name), "hello-dpumesh-%u", request_id);
        if (name_len < 0)
            goto argp_cleanup;
        if ((size_t)name_len >= sizeof(name))
            name_len = (int)sizeof(name) - 1;

        scores[0] = request_id;
        scores[1] = request_id + 1U;
        scores[2] = request_id + 2U;
        scores[3] = request_id + 3U;

        result = dmesh_grpc_hello_request_alloc(&app_arena,
                                                request_id,
                                                name,
                                                (uint32_t)name_len,
                                                scores,
                                                4,
                                                &req);
        if (result == DOCA_ERROR_NO_MEMORY) {
            dma_ring_refresh_consumer(objs->dma_ring);
            dmesh_grpc_arena_reclaim_through(&app_arena,
                                                objs->dma_ring->observed_consumer_seq);
            (void)doca_pe_progress(objs->producer_pe);
            continue;
        }
        if (result != DOCA_SUCCESS)
            goto argp_cleanup;

        result = dmesh_grpc_submit_request(objs,
                                            DMESH_GRPC_SCHEMA_HELLO_REQUEST,
                                            &app_arena,
                                            req,
                                            sizeof(*req),
                                            request_id);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to submit gRPC request descriptor: %s",
                         doca_error_get_descr(result));
            goto argp_cleanup;
        }

        request_id++;
    }

    DOCA_LOG_INFO("Finished Host worker");

argp_cleanup:
    if (objs != NULL && objs->grpc_offload == &app_arena)
        objs->grpc_offload = NULL;
    dmesh_grpc_arena_destroy(&app_arena);
    clean_argp();
// exit: 
    return;
}
