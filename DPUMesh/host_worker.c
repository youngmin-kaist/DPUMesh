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
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_mmap.h>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BUFFER_SIZE (1024 * 1024)

/* DPA kernel entry points (dpacc host stubs) */
extern doca_dpa_func_t thread_init_rpc;

/* defined in main.c */
extern double diff_sec(const struct timespec *start, const struct timespec *end);

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
    
    /* HTTP request mode: send exactly one request (<=8KB) as a single DMA
     * message, then idle to keep the connection open for the response path.
     * This exercises the outbound stack (detect/discovery/LB/mTLS) end to end.
     * DMESH_HTTP=1 sends a canned GET; DMESH_HTTP_REQ overrides the payload. */
    /* DMESH_HTTP_FILE=<path> sends the file's raw bytes (binary-safe: use this
     * for HTTP/2 prior-knowledge, gRPC, TLS ClientHello, etc.). Falls back to
     * DMESH_HTTP_REQ (text) or the canned GET under DMESH_HTTP=1. */
    const char *http_file = getenv("DMESH_HTTP_FILE");
    const char *http_req = getenv("DMESH_HTTP_REQ");
    if (http_req == NULL && http_file == NULL && getenv("DMESH_HTTP") != NULL)
        http_req = "GET / HTTP/1.1\r\nHost: dmesh\r\nConnection: close\r\n\r\n";
    if (http_file != NULL) {
        FILE *f = fopen(http_file, "rb");
        size_t len = 0;
        if (f == NULL) {
            DOCA_LOG_ERR("Failed to open DMESH_HTTP_FILE '%s'", http_file);
            return;
        }
        len = fread(objs->sndbuf.buf, 1, objs->sndbuf.size, f);
        fclose(f);
        desc = get_next_dma_desc(objs->dma_ring);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->sndbuf.buf;
        desc->size = len;
        commit_dma_desc(objs->dma_ring);
        DOCA_LOG_INFO("Host worker sent %zu bytes from %s; idling for response", len, http_file);
        while (true)
            sleep(3600);
        return;
    }
    if (http_req != NULL) {
        size_t len = strlen(http_req);
        if (len > objs->sndbuf.size)
            len = objs->sndbuf.size;
        memcpy(objs->sndbuf.buf, http_req, len);
        desc = get_next_dma_desc(objs->dma_ring);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->sndbuf.buf;
        desc->size = len;
        commit_dma_desc(objs->dma_ring);
        DOCA_LOG_INFO("Host worker sent HTTP request (%zu bytes); idling for response", len);
        while (true)
            sleep(3600);
        return;
    }

    int msg_size = 8192;
    const char *msg_size_env = getenv("DMESH_MSG_SIZE");
    if (msg_size_env != NULL && atoi(msg_size_env) > 0)
        msg_size = atoi(msg_size_env);
    DOCA_LOG_INFO("Host worker msg_size = %d bytes", msg_size);
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

/* One drain worker per host CPU core: busy-polls its own PE. Completion
 * callbacks run inside doca_pe_progress, so each drain group's counters
 * (its private struct objects) are only ever written by its own thread. */
struct host_bench_drain_ctx {
    struct doca_pe *pe;
    volatile int *stop;
};

static void *
host_bench_drain(void *a)
{
    struct host_bench_drain_ctx *ctx = a;

    while (!*ctx->stop)
        doca_pe_progress(ctx->pe);
    return NULL;
}

/*
 * Standalone host-launched DPA producer_dma_copy benchmark: the host CPU
 * creates the DPA instance, thread and comch msgqs itself (no DPU app, no
 * comch server involved) and launches the bench kernel. The DMA copies
 * host sndbuf -> host staging buffer; completion messages land on the host
 * CPU consumer, which gives the wall-clock rate. Configured via
 * DMESH_DPA_BENCH_MODE/SIZE/OPS/THREADS/CORES, like the DPU-launched variant.
 * DPA threads are distributed round-robin over CORES PEs, each drained by a
 * dedicated host pthread; the main thread only monitors the counters.
 */
static void
run_host_dpa_bench(const struct global_config *gcfg)
{
#define HOST_BENCH_MAX_THREADS 32
#define HOST_BENCH_MAX_CORES 8
    struct objects *objs;
    struct objects *gobjs[HOST_BENCH_MAX_CORES] = {0};
    struct doca_pe *pes[HOST_BENCH_MAX_CORES] = {0};
    pthread_t drain_threads[HOST_BENCH_MAX_CORES];
    struct host_bench_drain_ctx drain_ctxs[HOST_BENCH_MAX_CORES];
    volatile int drain_stop = 0;
    uint32_t bench_cores = 1;
    long total_recv;
    struct dmesh_conn *conns[HOST_BENCH_MAX_THREADS] = {0};
    struct dmesh_doca_dpa_thread *dpa_threads[HOST_BENCH_MAX_THREADS] = {0};
    struct dpa_thread_arg arg = {0};
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
    doca_dpa_dev_completion_t dpa_producer_comp;
    doca_dpa_dev_comch_producer_t dpa_producer;
    doca_dpa_dev_comch_consumer_t dpa_consumer;
    doca_dpa_dev_mmap_t src_mmap, dst_mmap;
    struct timespec t_start, t_now, t_first = {0}, t_rep = {0};
    long first_seen = 0, rep_cnt = 0, total_ops;
    uint64_t rpc_ret;
    doca_error_t result;
    const char *env;
    uint32_t bench_mode = 1, bench_size = 4096, bench_ops = 1000000, bench_threads = 1;
    uint32_t i;

    if ((env = getenv("DMESH_DPA_BENCH_MODE")) != NULL && atoi(env) > 0)
        bench_mode = (uint32_t)atoi(env);
    if ((env = getenv("DMESH_DPA_BENCH_SIZE")) != NULL && atoi(env) > 0)
        bench_size = (uint32_t)atoi(env);
    if ((env = getenv("DMESH_DPA_BENCH_OPS")) != NULL && atoi(env) > 0)
        bench_ops = (uint32_t)atoi(env);
    if ((env = getenv("DMESH_DPA_BENCH_THREADS")) != NULL && atoi(env) > 0)
        bench_threads = (uint32_t)atoi(env);
    if (bench_threads > HOST_BENCH_MAX_THREADS)
        bench_threads = HOST_BENCH_MAX_THREADS;
    if ((env = getenv("DMESH_DPA_BENCH_CORES")) != NULL && atoi(env) > 0)
        bench_cores = (uint32_t)atoi(env);
    if (bench_cores > HOST_BENCH_MAX_CORES)
        bench_cores = HOST_BENCH_MAX_CORES;
    if (bench_cores > bench_threads)
        bench_cores = bench_threads;
    total_ops = (long)bench_ops * bench_threads;

    objs = calloc(1, sizeof(*objs));
    if (objs == NULL)
        return;

    result = open_doca_device_with_pci(gcfg->dev_pci_addr, NULL, &objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to open DOCA device: %s", doca_error_get_descr(result));
        return;
    }

    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to init DPA (host-side DPA may be unsupported in this mode): %s",
                     doca_error_get_descr(result));
        return;
    }

    /* one PE per host drain core; each drain group gets a private counter
     * object (sharing the same doca_dev) so counters are single-writer */
    for (i = 0; i < bench_cores; i++) {
        result = doca_pe_create(&pes[i]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to create PE %u: %s", i, doca_error_get_descr(result));
            return;
        }
        gobjs[i] = calloc(1, sizeof(*gobjs[i]));
        if (gobjs[i] == NULL)
            return;
        gobjs[i]->dev = objs->dev;
    }

    /* shared DMA read source in host memory */
    result = init_dmesh_buffer(objs->dev, &objs->sndbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to init src buffer: %s", doca_error_get_descr(result));
        return;
    }
    result = doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &src_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to get src mmap handle: %s", doca_error_get_descr(result));
        return;
    }

    /* set up all DPA threads first so the later launch loop is tight and the
     * threads run concurrently with minimal start stagger */
    for (i = 0; i < bench_threads; i++) {
        struct dmesh_doca_dpa_thread *dpa_thread;
        struct dmesh_conn *conn;

        dpa_thread = calloc(1, sizeof(*dpa_thread));
        conn = calloc(1, sizeof(*conn));
        if (dpa_thread == NULL || conn == NULL)
            return;
        dpa_threads[i] = dpa_thread;
        conns[i] = conn;

        dpa_thread->dpa = objs->dpa_pool->dpa;
        result = dmesh_doca_dpa_thread_create(dpa_thread);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to create DPA thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        conn->objs = gobjs[i % bench_cores];
        conn->dpa_thread = dpa_thread;

        result = init_comch_dpa_msgq(conn, pes[i % bench_cores]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to init DPA msgqs for thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        /* per-thread staging destination */
        result = alloc_buffer_and_set_mmap(&conn->local_mmap, objs->dev,
                                           &conn->dma_buffer, BUFFER_SIZE,
                                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to alloc staging buffer %u: %s", i, doca_error_get_descr(result));
            return;
        }

        result = doca_comch_consumer_completion_get_dpa_handle(conn->dpa_comch->consumer_comp, &dpa_consumer_comp);
        if (result == DOCA_SUCCESS)
            result = doca_dpa_completion_get_dpa_handle(conn->dpa_comch->producer_comp, &dpa_producer_comp);
        if (result == DOCA_SUCCESS)
            result = doca_comch_consumer_get_dpa_handle(conn->dpa_comch->send.consumer, &dpa_consumer);
        if (result == DOCA_SUCCESS)
            result = doca_comch_producer_get_dpa_handle(conn->dpa_comch->recv.producer, &dpa_producer);
        if (result == DOCA_SUCCESS)
            result = doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dst_mmap);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to get DPA handles for thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        arg = (struct dpa_thread_arg) {
            .dpa_consumer_comp = dpa_consumer_comp,
            .dpa_producer_comp = dpa_producer_comp,
            .dpa_consumer = dpa_consumer,
            .dpa_producer = dpa_producer,
            .dpa_buf_arr = 0,               /* bench-only: no descriptor ring */
            .host_mmap = src_mmap,          /* DMA source: shared sndbuf */
            .dpu_mmap = dst_mmap,           /* DMA destination: staging */
            .src_addr = (uint64_t)conn->dma_buffer,
            .buf_size = BUFFER_SIZE,
            .bench_host_addr = (uint64_t)objs->sndbuf.buf,
            .bench_host_size = (uint32_t)objs->sndbuf.size,
            .bench_mode = bench_mode,
            .bench_msg_size = bench_size,
            .bench_num_ops = bench_ops,
        };

        result = doca_dpa_rpc(dpa_thread->dpa, thread_init_rpc, &rpc_ret,
                              arg.dpa_consumer, (uint32_t)CC_DPA_MAX_MSG_NUM);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: init RPC failed for thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg, &arg, sizeof(arg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to copy thread arg %u: %s", i, doca_error_get_descr(result));
            return;
        }
    }

    DOCA_LOG_INFO("host bench: launching %u DPA kernel(s) on %u drain core(s) (mode=%u size=%u ops=%u/thread)",
                  bench_threads, bench_cores, bench_mode, bench_size, bench_ops);
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* launch + kick all threads back to back; a thread only wakes on a
     * completion event, so the kick message plays the role that
     * send_dma_request_to_dpa has in the DPU flow */
    for (i = 0; i < bench_threads; i++) {
        struct comch_msg kick = {0};

        result = doca_dpa_thread_run(dpa_threads[i]->thread);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to run DPA thread %u: %s", i, doca_error_get_descr(result));
            return;
        }
        result = dmesh_doca_dpa_msgq_send(&conns[i]->dpa_comch->send, &kick, sizeof(kick));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to send kick message %u: %s", i, doca_error_get_descr(result));
            return;
        }
    }

    /* one drain pthread per host core; the main thread only monitors */
    for (i = 0; i < bench_cores; i++) {
        drain_ctxs[i].pe = pes[i];
        drain_ctxs[i].stop = &drain_stop;
        if (pthread_create(&drain_threads[i], NULL, host_bench_drain, &drain_ctxs[i]) != 0) {
            DOCA_LOG_ERR("host bench: failed to create drain thread %u", i);
            return;
        }
    }

    while (true) {
        total_recv = 0;
        for (i = 0; i < bench_cores; i++)
            total_recv += gobjs[i]->recv_msg_cnt;

        if (!first_seen && total_recv > 0) {
            clock_gettime(CLOCK_MONOTONIC, &t_first);
            t_rep = t_first;
            first_seen = 1;
        }
        if (total_recv >= total_ops) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double sec = diff_sec(&t_first, &t_now);
            printf("HOST_BENCH_DONE threads=%u cores=%u size=%u total_ops=%ld wall_sec=%.6f ops_per_sec=%.0f gbps=%.2f\n",
                   bench_threads, bench_cores, bench_size, total_ops, sec,
                   total_ops / sec,
                   (double)total_ops * bench_size * 8 / sec / 1e9);
            fflush(stdout);
            break;
        }

        clock_gettime(CLOCK_MONOTONIC, &t_now);
        if (diff_sec(&t_start, &t_now) >= 60.0) {
            printf("HOST_BENCH_TIMEOUT recv=%ld/%ld\n", total_recv, total_ops);
            fflush(stdout);
            break;
        }
        if (first_seen && diff_sec(&t_rep, &t_now) >= 1.0) {
            printf("HOST_BENCH_RATE %.0f ops/s (recv %ld/%ld)\n",
                   (total_recv - rep_cnt) / diff_sec(&t_rep, &t_now),
                   total_recv, total_ops);
            fflush(stdout);
            rep_cnt = total_recv;
            t_rep = t_now;
        }
        usleep(100);
    }

    drain_stop = 1;
    for (i = 0; i < bench_cores; i++)
        pthread_join(drain_threads[i], NULL);

    /* give the device log a moment to flush BENCH_* lines, then exit */
    sleep(1);
    exit(0);
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

    /* Flow identity for this worker's connection. A real host shim fills
     * this from the intercepted pod flow; the benchmark synthesizes it
     * (destination overridable via DMESH_DST_IP / DMESH_DST_PORT). */
    const char *dst_ip_env = getenv("DMESH_DST_IP");
    const char *dst_port_env = getenv("DMESH_DST_PORT");
    objs->flow.src_ip = inet_addr("127.0.0.1");
    objs->flow.src_port = (uint16_t)(40000 + ctx->idx);
    objs->flow.dst_ip = inet_addr(dst_ip_env != NULL ? dst_ip_env : "127.0.0.1");
    objs->flow.dst_port = (uint16_t)(dst_port_env != NULL ? atoi(dst_port_env) : 8080);
    snprintf(objs->flow.src_workload, sizeof(objs->flow.src_workload),
             "host-worker-%d", ctx->idx);

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

    /* host-launched DPA bench replaces the normal workers */
    if (getenv("DMESH_DPA_BENCH_MODE") != NULL) {
        run_host_dpa_bench(gcfg);
        return;
    }

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
