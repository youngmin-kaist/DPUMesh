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

#include "common.h"

#include <doca_log.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>
#include <time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_SIZE (1024 * 1024)

DOCA_LOG_REGISTER(HOST_WORKER);
void
run_host_worker(struct objects *objs, const char *server_name)
{
    doca_error_t result;

    DOCA_LOG_INFO("Starting Host worker, connecting to server '%s'", server_name);

    /* initialize DOCA Comch client objects */
    result = init_comch_ctrl_path_client(server_name, objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path client: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* initialize DOCA Comch producer objects */
    result = init_comch_datapath_producer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send message over comch data path: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* setup DMA ring */
    result = setup_dma_ring(objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DMA ring: %s", doca_error_get_descr(result));
        return;
    }

    /* allocate local buffer and set mmap for PCI export */
    result = init_dmesh_buffer(objs->dev, &objs->sndbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init dmesh buffer: %s", doca_error_get_descr(result));
        return;
    }

    result = init_dmesh_buffer(objs->dev, &objs->rcvbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init dmesh buffer: %s", doca_error_get_descr(result));
        return;
    }

    /* export DMA ring + send/receive buffer metadata to DPU in one message */
    result = export_dma_metadata(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export DMA metadata: %s", doca_error_get_descr(result));
        return;
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

    int idx = 9999;
    size_t pos = 0;
    struct dma_desc *desc;

    doca_dpa_dev_mmap_t local_mmap;
    doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &local_mmap);
    
    int msg_size = 8192;
    while (true) {
        // if (doca_pe_progress(objs->pe) == 0)
            // nanosleep(&ts, &ts);

        desc = get_next_dma_desc(objs->dma_ring);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->sndbuf.buf + pos;
        *(uint32_t *)desc->addr = idx--;
        desc->size = (size_t)msg_size;
        commit_dma_desc(objs->dma_ring);
        pos += msg_size;
        if (pos >= objs->sndbuf.size) {
            pos = 0;
        }
        // if (cur_ts.tv_nsec - prev_ts.tv_nsec >= 100) {
        //     prev_ts = cur_ts;
        // }
        // break;
    }

    DOCA_LOG_INFO("Finished Host worker");
}

struct host_worker_thread_ctx {
    const struct global_config *gcfg;
    int idx;
};

/*
 * One host worker thread = one connection to the DPU. Each thread opens its
 * own DOCA device handle and owns a private struct objects, so threads share
 * no DOCA state and need no locking.
 */
static void *
host_worker_thread(void *arg)
{
    struct host_worker_thread_ctx *ctx = arg;
    struct objects *objs;
    char server_name[32];
    doca_error_t result;

    objs = calloc(1, sizeof(*objs));
    if (objs == NULL) {
        DOCA_LOG_ERR("worker %d: failed to allocate objects", ctx->idx);
        return NULL;
    }

    result = open_doca_device_with_pci(ctx->gcfg->dev_pci_addr, NULL, &objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("worker %d: failed to open DOCA device: %s",
                     ctx->idx, doca_error_get_descr(result));
        free(objs);
        return NULL;
    }

    /* spread connections across the DPU worker servers round-robin */
    int num_dpu_workers = ctx->gcfg->num_dpu_workers > 0 ? ctx->gcfg->num_dpu_workers : 1;
    snprintf(server_name, sizeof(server_name), "DPUMesh%d", ctx->idx % num_dpu_workers);

    DOCA_LOG_INFO("worker %d: connecting to DPU server '%s'", ctx->idx, server_name);
    run_host_worker(objs, server_name);

    /* run_host_worker only returns on error (the data loop never exits) */
    DOCA_LOG_ERR("worker %d: exited", ctx->idx);
    free(objs);
    return NULL;
}

void
run_host_workers(const struct global_config *gcfg)
{
    pthread_t *threads;
    struct host_worker_thread_ctx *ctxs;
    int num_threads = gcfg->num_threads > 0 ? gcfg->num_threads : 1;
    int i;

    DOCA_LOG_INFO("Starting %d host worker thread(s), one connection each", num_threads);

    threads = calloc(num_threads, sizeof(*threads));
    ctxs = calloc(num_threads, sizeof(*ctxs));
    if (threads == NULL || ctxs == NULL) {
        DOCA_LOG_ERR("Failed to allocate worker thread state");
        free(threads);
        free(ctxs);
        return;
    }

    for (i = 0; i < num_threads; i++) {
        ctxs[i].gcfg = gcfg;
        ctxs[i].idx = i;
        if (pthread_create(&threads[i], NULL, host_worker_thread, &ctxs[i]) != 0) {
            DOCA_LOG_ERR("Failed to create host worker thread %d", i);
            break;
        }
    }

    /* workers run forever; join blocks unless a worker errors out */
    for (i = i - 1; i >= 0; i--)
        pthread_join(threads[i], NULL);

    free(threads);
    free(ctxs);
}
