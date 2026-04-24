#include "../grpc_shard_routing.h"

#include <algorithm>
#include <cstdio>
#include <vector>

namespace {

static ProtoTask make_task(uint32_t request_id)
{
    ProtoTask task{};

    task.request_id = request_id;
    task.desc_id = 1U;
    return task;
}

static bool run_dispatch_plan_test(uint32_t shard_count)
{
    std::vector<ProtoTask> tasks = {
        make_task(100U),
        make_task(777U),
        make_task(5000U),
        make_task(42U),
        make_task(123456U),
        make_task(17U),
    };
    GrpcShardDispatchPlan plan;
    uint32_t routed = 0U;

    if (!grpc_build_shard_dispatch_plan(tasks, shard_count, &plan))
        return false;
    if (plan.shard_count != shard_count)
        return false;
    if (plan.shard_by_task.size() != tasks.size())
        return false;
    if (plan.task_indices_by_shard.size() != shard_count)
        return false;

    for (uint32_t i = 0; i < tasks.size(); ++i) {
        uint32_t expected = grpc_pick_ring_shard(tasks[i].request_id, shard_count);

        if (plan.shard_by_task[i] != expected)
            return false;
    }

    for (uint32_t shard = 0; shard < shard_count; ++shard) {
        for (uint32_t idx : plan.task_indices_by_shard[shard]) {
            if (plan.shard_by_task[idx] != shard)
                return false;
            routed++;
        }
    }

    return routed == tasks.size();
}

static bool run_out_of_order_completion_test()
{
    std::vector<ProtoTask> tasks = {
        make_task(100U),
        make_task(777U),
        make_task(5000U),
        make_task(42U),
    };
    std::vector<uint32_t> keys;
    std::vector<uint32_t> values;
    std::vector<uint32_t> completions = {5000U, 42U, 100U, 777U};
    std::vector<uint32_t> seen(tasks.size(), 0U);

    if (!grpc_build_request_index_map(tasks, keys, values))
        return false;

    for (uint32_t request_id : completions) {
        uint32_t task_idx = 0U;

        if (!grpc_lookup_request_index(keys, values, request_id, &task_idx))
            return false;
        if (task_idx >= tasks.size())
            return false;
        if (tasks[task_idx].request_id != request_id)
            return false;
        seen[task_idx]++;
    }

    return std::all_of(seen.begin(), seen.end(), [](uint32_t v) { return v == 1U; });
}

static bool run_shard_failure_isolation_test()
{
    std::vector<ProtoTask> tasks = {
        make_task(1U),
        make_task(2U),
        make_task(3U),
        make_task(4U),
        make_task(5U),
        make_task(6U),
    };
    GrpcShardDispatchPlan plan;
    std::vector<int32_t> status(tasks.size(), 0);
    uint32_t failed_shard = 1U;
    bool has_success = false;
    bool has_failure = false;

    if (!grpc_build_shard_dispatch_plan(tasks, 2U, &plan))
        return false;

    for (uint32_t shard = 0; shard < 2U; ++shard) {
        for (uint32_t idx : plan.task_indices_by_shard[shard]) {
            status[idx] = (shard == failed_shard) ? -100 : 0;
        }
    }

    for (uint32_t i = 0; i < tasks.size(); ++i) {
        uint32_t shard = plan.shard_by_task[i];

        if (shard == failed_shard) {
            if (status[i] != -100)
                return false;
            has_failure = true;
        } else {
            if (status[i] != 0)
                return false;
            has_success = true;
        }
    }

    return has_success && has_failure;
}

} // namespace

int main()
{
    if (!run_dispatch_plan_test(2U))
        return 1;
    if (!run_dispatch_plan_test(4U))
        return 1;
    if (!run_out_of_order_completion_test())
        return 1;
    if (!run_shard_failure_isolation_test())
        return 1;

    std::printf("grpc_sharded_worker_test: ok\n");
    return 0;
}
