#include "grpc_dpa_offload.h"

#include <doca_error.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * This symbol is generated from the DPA device program and linked on host side
 * by dpacc output (similar to thread_init_rpc in existing DPUMesh code).
 */
extern uint64_t grpc_dpa_serialize_batch_rpc(uint64_t worker_arg_addr, uint32_t num_tasks);

#ifdef GRPC_DPA_ENABLE_RING_LOOP
#include "grpc_ring_loop.h"
extern int ring_pop_task(ProtoTask *task);                    /* host producer ring */
extern void ring_push_completion(const ProtoCompletion *cpl); /* host completion ring */
#endif

static doca_error_t
copy_task_payloads_to_dpa(struct doca_dpa *dpa, const ProtoTask *tasks, uint32_t n)
{
    doca_error_t result;
    uint32_t i;

    for (i = 0; i < n; ++i) {
        result = doca_dpa_h2d_memcpy(dpa,
                                     tasks[i].dpa_msg_addr,
                                     (void *)tasks[i].host_msg_addr,
                                     tasks[i].host_msg_len);
        if (result != DOCA_SUCCESS)
            return result;
    }

    return DOCA_SUCCESS;
}

static doca_error_t
copy_task_outputs_from_dpa(struct doca_dpa *dpa,
                           const ProtoTask *tasks,
                           const ProtoCompletion *cpls,
                           uint32_t n)
{
    doca_error_t result;
    uint32_t i;

    for (i = 0; i < n; ++i) {
        if (cpls[i].status != 0)
            continue;
        if (cpls[i].encoded_len > tasks[i].host_out_cap)
            return DOCA_ERROR_INVALID_VALUE;

        result = doca_dpa_d2h_memcpy(dpa,
                                     tasks[i].host_out_addr,
                                     tasks[i].dpa_out_addr,
                                     cpls[i].encoded_len);
        if (result != DOCA_SUCCESS)
            return result;
    }

    return DOCA_SUCCESS;
}

static doca_error_t
grpc_dpa_submit_batch_rpc_path(GrpcDpaOffloadCtx *ctx,
                               const ProtoTask *tasks,
                               uint32_t num_tasks,
                               ProtoCompletion *cpls)
{
    doca_error_t result;
    uint64_t rpc_ret = 0;

    result = copy_task_payloads_to_dpa(ctx->dpa, tasks, num_tasks);
    if (result != DOCA_SUCCESS)
        return result;

    result = doca_dpa_h2d_memcpy(ctx->dpa,
                                 ctx->worker_arg.task_array_addr,
                                 (void *)tasks,
                                 num_tasks * sizeof(*tasks));
    if (result != DOCA_SUCCESS)
        return result;

    result = doca_dpa_rpc(ctx->dpa,
                          (doca_dpa_func_t *)grpc_dpa_serialize_batch_rpc,
                          &rpc_ret,
                          ctx->dpa_worker_arg_addr,
                          num_tasks);
    if (result != DOCA_SUCCESS)
        return result;
    if (rpc_ret != 0)
        return DOCA_ERROR_DRIVER;

    result = doca_dpa_d2h_memcpy(ctx->dpa,
                                 cpls,
                                 ctx->worker_arg.completion_array_addr,
                                 num_tasks * sizeof(*cpls));
    if (result != DOCA_SUCCESS)
        return result;

    return copy_task_outputs_from_dpa(ctx->dpa, tasks, cpls, num_tasks);
}

doca_error_t
grpc_dpa_offload_init(GrpcDpaOffloadCtx *ctx,
                      struct doca_dpa *dpa,
                      uint64_t dpa_worker_arg_addr,
                      uint64_t dpa_desc_blob_addr,
                      uint64_t dpa_task_array_addr,
                      uint64_t dpa_completion_array_addr,
                      uint32_t max_batch)
{
    if (ctx == NULL || dpa == NULL || max_batch == 0)
        return DOCA_ERROR_INVALID_VALUE;

    memset(ctx, 0, sizeof(*ctx));
    ctx->dpa = dpa;
    ctx->dpa_worker_arg_addr = dpa_worker_arg_addr;
    ctx->worker_arg = (GrpcDpaWorkerArg){
        .desc_blob_addr = dpa_desc_blob_addr,
        .task_array_addr = dpa_task_array_addr,
        .completion_array_addr = dpa_completion_array_addr,
        .max_batch = max_batch,
    };

    return doca_dpa_h2d_memcpy(ctx->dpa,
                               ctx->dpa_worker_arg_addr,
                               &ctx->worker_arg,
                               sizeof(ctx->worker_arg));
}

doca_error_t
grpc_dpa_push_desc_blob(GrpcDpaOffloadCtx *ctx, const ProtoDescBlob *blob)
{
    if (ctx == NULL || blob == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    return doca_dpa_h2d_memcpy(ctx->dpa,
                               ctx->worker_arg.desc_blob_addr,
                               (void *)blob,
                               sizeof(*blob));
}

doca_error_t
grpc_dpa_submit_batch(GrpcDpaOffloadCtx *ctx,
                      const ProtoTask *tasks,
                      uint32_t num_tasks,
                      ProtoCompletion *cpls)
{
    if (ctx == NULL || tasks == NULL || cpls == NULL || num_tasks == 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (num_tasks > ctx->worker_arg.max_batch)
        return DOCA_ERROR_INVALID_VALUE;

    return grpc_dpa_submit_batch_rpc_path(ctx, tasks, num_tasks, cpls);
}

#ifdef GRPC_DPA_ENABLE_RING_LOOP
static void
fill_batch_error_completions(const ProtoTask *tasks,
                             uint32_t n,
                             int32_t status,
                             ProtoCompletion *cpls)
{
    uint32_t i;

    for (i = 0; i < n; ++i) {
        cpls[i].request_id = tasks[i].request_id;
        cpls[i].encoded_len = 0U;
        cpls[i].status = status;
    }
}

void
grpc_dpa_batch_loop(GrpcDpaOffloadCtx *ctx)
{
    ProtoTask *tasks;
    ProtoCompletion *cpls;
    uint32_t max_batch;
    uint32_t n;

    if (ctx == NULL || ctx->worker_arg.max_batch == 0U)
        return;

    max_batch = ctx->worker_arg.max_batch;
    tasks = (ProtoTask *)calloc(max_batch, sizeof(*tasks));
    cpls = (ProtoCompletion *)calloc(max_batch, sizeof(*cpls));
    if (tasks == NULL || cpls == NULL) {
        free(tasks);
        free(cpls);
        return;
    }

    for (;;) {
        if (grpc_ring_is_shutdown())
            break;

        n = 0;
        while (n < max_batch && ring_pop_task(&tasks[n]))
            ++n;

        if (n == 0)
            continue;

        {
            doca_error_t result = grpc_dpa_submit_batch_rpc_path(ctx, tasks, n, cpls);

            if (result != DOCA_SUCCESS)
                fill_batch_error_completions(tasks, n, -100, cpls);
        }

        for (uint32_t i = 0; i < n; ++i)
            ring_push_completion(&cpls[i]);
    }

    free(tasks);
    free(cpls);
}
#endif
