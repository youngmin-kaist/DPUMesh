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
#include "grpc/grpc_offload.h"

#include <doca_log.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

DOCA_LOG_REGISTER(HOST_WORKER);

static inline uint64_t
random_chars_next(uint64_t *state)
{
    uint64_t x = *state;

    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;

    return x * UINT64_C(2685821657736338717);
}

static inline uint64_t
random_chars_seed(void)
{
    uint64_t seed = (uint64_t)time(NULL);

    seed ^= (uint64_t)clock() << 32;
    seed ^= (uint64_t)(uintptr_t)&seed;

    if (seed == 0)
        seed = UINT64_C(0x9e3779b97f4a7c15);

    return seed;
}

static inline void
fill_random_chars(char *buf, size_t len)
{
    static _Thread_local uint64_t state;
    size_t offset = 0;

    if (buf == NULL || len == 0)
        return;

    if (state == 0)
        state = random_chars_seed();

    while (len - offset >= sizeof(uint64_t)) {
        uint64_t value = random_chars_next(&state);

        memcpy(buf + offset, &value, sizeof(value));
        offset += sizeof(value);
    }

    if (offset < len) {
        uint64_t value = random_chars_next(&state);

        memcpy(buf + offset, &value, len - offset);
    }
}

static inline void
fill_random_numbers(uint32_t *buf, int count)
{
    static _Thread_local uint64_t state;
    int i = 0;

    if (buf == NULL || count <= 0)
        return;

    if (state == 0)
        state = random_chars_seed();

    while (i + 1 < count) {
        uint64_t value = random_chars_next(&state);

        buf[i++] = (uint32_t)value;
        buf[i++] = (uint32_t)(value >> 32);
    }

    if (i < count)
        buf[i] = (uint32_t)random_chars_next(&state);
}

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
    result = alloc_hugepage_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
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
    
    while (doca_pe_progress(objs->pe) > 0) {
        /* drain completions / keep host side progressing */
    }

    char *name;
    int name_len = 8;
    if (name_len > 0) {
        name = (char *)malloc((size_t)name_len + 1);
        if (name == NULL) {
            DOCA_LOG_ERR("Failed to allocate name buffer");
            goto argp_cleanup;
        }
        fill_random_chars(name, name_len);
    }

    uint32_t *scores = NULL;
    int scores_count = 0;
    if (scores_count > 0) {
        scores = (uint32_t *)malloc((size_t)scores_count * sizeof(uint32_t));
        if (scores == NULL) {
            DOCA_LOG_ERR("Failed to allocate scores buffer");
            goto argp_cleanup;
        }
        fill_random_numbers(scores, scores_count);
    }

    while (true) {
        struct dmesh_grpc_hello_flat *flat = NULL;
        size_t flat_len = 0;
        // struct dmesh_grpc_hello_request *req;
        // result = dmesh_grpc_hello_request_alloc(&app_arena,
        //                                         request_id,
        //                                         name,
        //                                         (uint32_t)name_len,
        //                                         scores,
        //                                         (uint32_t)scores_count,
        //                                         &req);
        result = dmesh_grpc_hello_flat_alloc(&app_arena,
                                             request_id,
                                             name,
                                             (uint32_t)name_len,
                                             scores,
                                             (uint32_t)scores_count,
                                             &flat,
                                             &flat_len);

        if (result == DOCA_ERROR_NO_MEMORY) {
            dma_ring_refresh_consumer(objs->dma_ring);
            dmesh_grpc_arena_reclaim_through(&app_arena,
                                                objs->dma_ring->observed_consumer_seq);
            continue;
        }
        if (result != DOCA_SUCCESS)
            goto argp_cleanup;

        result = dmesh_grpc_submit_request(objs,
                                            DMESH_GRPC_SCHEMA_HELLO_REQUEST,
                                            &app_arena,
                                            flat,
                                            flat_len,
                                            request_id);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to submit gRPC request descriptor: %s",
                         doca_error_get_descr(result));
            goto argp_cleanup;
        }

        request_id = (request_id + 1) % 1000000;
        if (request_id == 1) {
            DOCA_LOG_INFO("%lu %lu\n", objs->dma_ring->producer_seq, objs->dma_ring->observed_consumer_seq);
        }
    }

    DOCA_LOG_INFO("Finished Host worker");

argp_cleanup:
    if (objs != NULL && objs->grpc_offload == &app_arena)
        objs->grpc_offload = NULL;
    free(scores);
    dmesh_grpc_arena_destroy(&app_arena);
    if (objs != NULL) {
        destroy_mmap_and_unmap_hugepage_buffer(objs->local_mmap, objs->dma_buffer, BUFFER_SIZE);
        objs->local_mmap = NULL;
        objs->dma_buffer = NULL;
    }
    clean_argp();
// exit: 
    return;
}
