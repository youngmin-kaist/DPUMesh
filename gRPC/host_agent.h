#pragma once

#include <doca_dpa.h>
#include <doca_error.h>

#include <cstdint>
#include <pthread.h>
#include <string>
#include <vector>

#include "grpc_dpa_offload.h"
#include "grpc_shard_routing.h"
#include "grpc_selective_policy_runtime.h"

#ifndef GRPC_DPA_MAX_ENCODED_BUF_SIZE
#define GRPC_DPA_MAX_ENCODED_BUF_SIZE (8192U)
#endif

#ifndef GRPC_DPA_FLAT_ARENA_SIZE
#define GRPC_DPA_FLAT_ARENA_SIZE GRPC_DPA_MAX_ENCODED_BUF_SIZE
#endif

#ifndef GRPC_DPA_MAX_FLAT_MSG_SIZE
#define GRPC_DPA_MAX_FLAT_MSG_SIZE GRPC_DPA_FLAT_ARENA_SIZE
#endif

#ifndef GRPC_DPA_SELECTIVE_STRING_THRESHOLD
#define GRPC_DPA_SELECTIVE_STRING_THRESHOLD 128U
#endif

#ifndef GRPC_DPA_RING_SHARD_COUNT
#define GRPC_DPA_RING_SHARD_COUNT 1U
#endif

namespace demo {
struct HelloRequest {
    uint64_t id_;
    std::string name_;
    std::vector<uint32_t> scores_;

    uint64_t id() const { return id_; }
    const std::string &name() const { return name_; }
    const std::vector<uint32_t> &scores() const { return scores_; }
};
} // namespace demo

#ifdef GRPC_DPA_ENABLE_RING_LOOP
struct GrpcRingShardRuntime {
    uint32_t shard_id;
    uint64_t dpa_worker_arg_addr;
    uint64_t dpa_ring_ctrl_addr;
    uint64_t dpa_req_ring_addr;
    uint64_t dpa_cpl_ring_addr;
    uint64_t dpa_msg_pool_addr;
    uint64_t dpa_out_pool_addr;
    struct doca_dpa_thread *thread;
    GrpcRingCtrl host_ring_ctrl;
    std::vector<GrpcReqDesc> host_req_ring;
    std::vector<GrpcCplDesc> host_cpl_ring;
    uint64_t completions_seen;
};
#endif

struct GrpcProtoRuntime {
    doca_dpa *dpa;
    doca_dpa_thread *thread;
    uint32_t max_batch;
    uint32_t ring_depth;
    uint32_t shard_count;

    uint64_t dpa_worker_arg_addr;
    uint64_t dpa_desc_blob_addr;
    uint64_t dpa_task_array_addr;
    uint64_t dpa_completion_array_addr;
    uint64_t dpa_msg_scratch_addr;
    uint64_t dpa_out_scratch_addr;

    uint64_t dpa_ring_ctrl_addr;
    uint64_t dpa_req_ring_addr;
    uint64_t dpa_cpl_ring_addr;
    struct doca_dpa_thread *ring_thread;

    GrpcRingCtrl host_ring_ctrl;
    std::vector<GrpcReqDesc> host_req_ring;
    std::vector<GrpcCplDesc> host_cpl_ring;

    std::vector<uint8_t> host_flat_scratch;
    std::vector<uint8_t> host_out_scratch;

    GrpcSelectiveOffloadPolicyConfig selective_policy;
    GrpcSelectiveOffloadPolicyStats selective_policy_stats;

    GrpcDpaOffloadCtx offload;

#ifdef GRPC_DPA_ENABLE_RING_LOOP
    std::vector<GrpcRingShardRuntime> shards;
#endif
};

doca_error_t grpc_proto_runtime_init(GrpcProtoRuntime *rt,
                                     doca_dpa *dpa,
                                     doca_dpa_thread *thread,
                                     uint32_t max_batch);

doca_error_t grpc_proto_runtime_init_ex(GrpcProtoRuntime *rt,
                                        doca_dpa *dpa,
                                        doca_dpa_thread *thread,
                                        uint32_t max_batch,
                                        uint32_t shard_count);

void grpc_proto_runtime_destroy(GrpcProtoRuntime *rt);

doca_error_t grpc_proto_serialize_hello(GrpcProtoRuntime *rt,
                                        const demo::HelloRequest &req,
                                        uint32_t request_id,
                                        std::vector<uint8_t> &encoded,
                                        ProtoCompletion *cpl_out);

doca_error_t grpc_proto_serialize_hello_batch(GrpcProtoRuntime *rt,
                                              const std::vector<demo::HelloRequest> &reqs,
                                              const std::vector<uint32_t> &request_ids,
                                              std::vector<std::vector<uint8_t>> &encoded_batch,
                                              std::vector<ProtoCompletion> *cpls_out);
