#include "grpc_shard_routing.h"

#include <algorithm>

namespace {

static uint32_t next_pow2(uint32_t v)
{
    uint32_t out = 1U;

    while (out < v)
        out <<= 1U;
    return out;
}

static uint32_t hash_request_id(uint32_t request_id)
{
    uint32_t x = request_id;

    x ^= x >> 16U;
    x *= 0x7feb352dU;
    x ^= x >> 15U;
    x *= 0x846ca68bU;
    x ^= x >> 16U;
    return x;
}

} // namespace

uint32_t grpc_pick_ring_shard(uint32_t request_id, uint32_t shard_count)
{
    if (shard_count == 0U)
        return 0U;
    return hash_request_id(request_id) % shard_count;
}

bool grpc_build_shard_dispatch_plan(const std::vector<ProtoTask> &tasks,
                                    uint32_t shard_count,
                                    GrpcShardDispatchPlan *plan)
{
    if (plan == nullptr || shard_count == 0U)
        return false;

    plan->shard_count = shard_count;
    plan->shard_by_task.assign(tasks.size(), 0U);
    plan->task_indices_by_shard.assign(shard_count, {});

    for (uint32_t i = 0; i < tasks.size(); ++i) {
        uint32_t shard = grpc_pick_ring_shard(tasks[i].request_id, shard_count);
        plan->shard_by_task[i] = shard;
        plan->task_indices_by_shard[shard].push_back(i);
    }

    return true;
}

bool grpc_build_request_index_map(const std::vector<ProtoTask> &tasks,
                                  std::vector<uint32_t> &keys,
                                  std::vector<uint32_t> &values)
{
    uint32_t cap;

    cap = next_pow2(std::max<uint32_t>(8U, static_cast<uint32_t>(tasks.size() * 2U)));
    keys.assign(cap, 0xffffffffU);
    values.assign(cap, 0xffffffffU);

    for (uint32_t i = 0; i < tasks.size(); ++i) {
        uint32_t mask = cap - 1U;
        uint32_t pos = hash_request_id(tasks[i].request_id) & mask;

        while (keys[pos] != 0xffffffffU) {
            if (keys[pos] == tasks[i].request_id)
                return false;
            pos = (pos + 1U) & mask;
        }

        keys[pos] = tasks[i].request_id;
        values[pos] = i;
    }

    return true;
}

bool grpc_lookup_request_index(const std::vector<uint32_t> &keys,
                               const std::vector<uint32_t> &values,
                               uint32_t request_id,
                               uint32_t *task_index)
{
    uint32_t mask;
    uint32_t pos;

    if (task_index == nullptr || keys.empty() || keys.size() != values.size())
        return false;

    mask = static_cast<uint32_t>(keys.size()) - 1U;
    pos = hash_request_id(request_id) & mask;

    while (keys[pos] != 0xffffffffU) {
        if (keys[pos] == request_id) {
            *task_index = values[pos];
            return true;
        }
        pos = (pos + 1U) & mask;
    }

    return false;
}
