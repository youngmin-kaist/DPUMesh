#pragma once

#include <doca_dpa.h>
#include <doca_error.h>

#include "proto_meta.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    struct doca_dpa *dpa;
    uint64_t dpa_worker_arg_addr;
    GrpcDpaWorkerArg worker_arg;
} GrpcDpaOffloadCtx;

doca_error_t grpc_dpa_offload_init(GrpcDpaOffloadCtx *ctx,
                                   struct doca_dpa *dpa,
                                   uint64_t dpa_worker_arg_addr,
                                   uint64_t dpa_desc_blob_addr,
                                   uint64_t dpa_task_array_addr,
                                   uint64_t dpa_completion_array_addr,
                                   uint32_t max_batch);

doca_error_t grpc_dpa_push_desc_blob(GrpcDpaOffloadCtx *ctx, const ProtoDescBlob *blob);

doca_error_t grpc_dpa_submit_batch(GrpcDpaOffloadCtx *ctx,
                                   const ProtoTask *tasks,
                                   uint32_t num_tasks,
                                   ProtoCompletion *cpls);

void grpc_dpa_batch_loop(GrpcDpaOffloadCtx *ctx);

#ifdef __cplusplus
}
#endif
