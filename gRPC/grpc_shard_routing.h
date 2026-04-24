#pragma once

#include "proto_meta.h"

#include <cstdint>
#include <vector>

struct GrpcShardDispatchPlan {
    uint32_t shard_count;
    std::vector<uint32_t> shard_by_task;
    std::vector<std::vector<uint32_t>> task_indices_by_shard;
};

uint32_t grpc_pick_ring_shard(uint32_t request_id, uint32_t shard_count);

bool grpc_build_shard_dispatch_plan(const std::vector<ProtoTask> &tasks,
                                    uint32_t shard_count,
                                    GrpcShardDispatchPlan *plan);

bool grpc_build_request_index_map(const std::vector<ProtoTask> &tasks,
                                  std::vector<uint32_t> &keys,
                                  std::vector<uint32_t> &values);

bool grpc_lookup_request_index(const std::vector<uint32_t> &keys,
                               const std::vector<uint32_t> &values,
                               uint32_t request_id,
                               uint32_t *task_index);
