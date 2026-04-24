#include "host_agent.h"

#include <doca_dpa.h>
#include <doca_error.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef GRPC_DPA_ENABLE_RING_LOOP
extern "C" {
extern doca_dpa_func_t grpc_dpa_worker_main;
}
#endif
struct FlatArena {
    uint8_t *storage;
    size_t cap;
    size_t off;

    explicit FlatArena(uint8_t *buf, size_t capacity) : storage(buf), cap(capacity), off(0) {}

    void *alloc(size_t n, size_t align = 8)
    {
        size_t p = (off + align - 1) & ~(align - 1);
        if (p + n > cap)
            throw std::bad_alloc();
        void *ret = storage + p;
        off = p + n;
        return ret;
    }

    template <typename T>
    T *alloc_obj()
    {
        return reinterpret_cast<T *>(alloc(sizeof(T), alignof(T)));
    }

    uint32_t offset_of(const void *p) const
    {
        auto base = reinterpret_cast<uintptr_t>(storage);
        auto cur = reinterpret_cast<uintptr_t>(p);
        return static_cast<uint32_t>(cur - base);
    }
};

static ProtoDescBlob build_desc_blob()
{
    ProtoDescBlob blob{};

    blob.msg_count = 1;
    blob.field_count = 3;

    blob.msgs[0].desc_id = 1;
    blob.msgs[0].field_begin = 0;
    blob.msgs[0].field_count = 3;
    blob.msgs[0].flat_size = sizeof(HelloRequestFlat);

    blob.fields[0].field_no = 1;
    blob.fields[0].kind = FK_U64;
    blob.fields[0].reserved0 = 0;
    blob.fields[0].reserved1 = 0;
    blob.fields[0].offset = static_cast<uint32_t>(offsetof(HelloRequestFlat, id));
    blob.fields[0].child_desc_id = 0;

    blob.fields[1].field_no = 2;
    blob.fields[1].kind = FK_STRING;
    blob.fields[1].reserved0 = 0;
    blob.fields[1].reserved1 = 0;
    blob.fields[1].offset = static_cast<uint32_t>(offsetof(HelloRequestFlat, name));
    blob.fields[1].child_desc_id = 0;

    blob.fields[2].field_no = 3;
    blob.fields[2].kind = FK_REPEATED_U32_PACKED;
    blob.fields[2].reserved0 = 0;
    blob.fields[2].reserved1 = 0;
    blob.fields[2].offset = static_cast<uint32_t>(offsetof(HelloRequestFlat, scores));
    blob.fields[2].child_desc_id = 0;

    return blob;
}

static void flatten_hello_request(const demo::HelloRequest &in,
                                  GrpcProtoRuntime *rt,
                                  FlatArena &arena,
                                  const void **msg_addr,
                                  uint32_t *msg_len)
{
    auto *flat = arena.alloc_obj<HelloRequestFlat>();
    flat->id = in.id();

    if (!in.name().empty()) {
        GrpcFieldPlacementDecision decision =
            grpc_selective_policy_decide_string(&rt->selective_policy,
                                                static_cast<uint32_t>(in.name().size()),
                                                0U);
        grpc_selective_policy_stats_record(&rt->selective_policy_stats, &decision);

        if (in.name().size() > PROTO_MAX_LEN_DELIMITED)
            throw std::length_error("name length exceeds 2GB");

        auto *name_buf = reinterpret_cast<uint8_t *>(arena.alloc(in.name().size(), 1));
        std::memcpy(name_buf, in.name().data(), in.name().size());
        
        flat->name.offset = arena.offset_of(name_buf);
        flat->name.len = static_cast<uint32_t>(in.name().size());
    } else {
        flat->name.offset = 0;
        flat->name.len = 0;
    }

    if (!in.scores().empty()) {
        auto *score_buf = reinterpret_cast<uint32_t *>(
            arena.alloc(in.scores().size() * sizeof(uint32_t), alignof(uint32_t)));
        std::memcpy(score_buf, in.scores().data(), in.scores().size() * sizeof(uint32_t));

        flat->scores.offset = arena.offset_of(score_buf);
        flat->scores.count = static_cast<uint32_t>(in.scores().size());
    } else {
        flat->scores.offset = 0;
        flat->scores.count = 0;
    }

    *msg_addr = flat;
    *msg_len = static_cast<uint32_t>(arena.off);
}

static inline uint64_t add_u64_offset(uint64_t base, uint64_t off)
{
    return base + off;
}

static doca_error_t prepare_batch_tasks(GrpcProtoRuntime *rt,
                                        const std::vector<demo::HelloRequest> &reqs,
                                        const std::vector<uint32_t> &request_ids,
                                        std::vector<ProtoTask> &tasks)
{
    const uint64_t msg_stride = static_cast<uint64_t>(GRPC_DPA_MAX_FLAT_MSG_SIZE);
    const uint64_t out_stride = static_cast<uint64_t>(GRPC_DPA_MAX_ENCODED_BUF_SIZE);

    if (reqs.size() != request_ids.size())
        return DOCA_ERROR_INVALID_VALUE;

    tasks.resize(reqs.size());

    for (size_t i = 0; i < reqs.size(); ++i) {
        for (size_t j = 0; j < i; ++j) {
            if (request_ids[j] == request_ids[i])
                return DOCA_ERROR_INVALID_VALUE;
        }

        FlatArena arena(rt->host_flat_scratch.data() + i * GRPC_DPA_MAX_FLAT_MSG_SIZE,
                        GRPC_DPA_MAX_FLAT_MSG_SIZE);
        const void *msg_addr = nullptr;
        uint32_t msg_len = 0;

        try {
            flatten_hello_request(reqs[i], rt, arena, &msg_addr, &msg_len);
        } catch (const std::bad_alloc &) {
            return DOCA_ERROR_NO_MEMORY;
        } catch (const std::length_error &) {
            return DOCA_ERROR_INVALID_VALUE;
        }

        if (msg_len > GRPC_DPA_MAX_FLAT_MSG_SIZE)
            return DOCA_ERROR_INVALID_VALUE;

        tasks[i] = ProtoTask{};
        tasks[i].desc_id = 1;
        tasks[i].host_msg_addr = msg_addr;
        tasks[i].host_msg_len = msg_len;
        tasks[i].host_out_addr = rt->host_out_scratch.data() + i * GRPC_DPA_MAX_ENCODED_BUF_SIZE;
        tasks[i].host_out_cap = GRPC_DPA_MAX_ENCODED_BUF_SIZE;
        tasks[i].dpa_out_cap = GRPC_DPA_MAX_ENCODED_BUF_SIZE;
        tasks[i].request_id = request_ids[i];
        tasks[i].dpa_msg_addr = add_u64_offset(rt->dpa_msg_scratch_addr, msg_stride * i);
        tasks[i].dpa_out_addr = add_u64_offset(rt->dpa_out_scratch_addr, out_stride * i);
    }

    return DOCA_SUCCESS;
}

#ifdef GRPC_DPA_ENABLE_RING_LOOP
static doca_error_t ring_ctrl_sync_h2d(GrpcProtoRuntime *rt, GrpcRingShardRuntime &shard)
{
    return doca_dpa_h2d_memcpy(rt->dpa, shard.dpa_ring_ctrl_addr, &shard.host_ring_ctrl, sizeof(shard.host_ring_ctrl));
}

static doca_error_t ring_req_sync_h2d(GrpcProtoRuntime *rt, const GrpcRingShardRuntime &shard, uint32_t idx)
{
    return doca_dpa_h2d_memcpy(rt->dpa,
                               shard.dpa_req_ring_addr + ((uint64_t)idx * sizeof(GrpcReqDesc)),
                               &shard.host_req_ring[idx],
                               sizeof(GrpcReqDesc));
}

static doca_error_t ring_cpl_sync_d2h(GrpcProtoRuntime *rt, GrpcRingShardRuntime &shard, uint32_t idx)
{
    return doca_dpa_d2h_memcpy(rt->dpa,
                               &shard.host_cpl_ring[idx],
                               shard.dpa_cpl_ring_addr + ((uint64_t)idx * sizeof(GrpcCplDesc)),
                               sizeof(GrpcCplDesc));
}

static void fill_error_completion_for_task(const ProtoTask &task, int32_t status, ProtoCompletion &cpl)
{
    cpl.request_id = task.request_id;
    cpl.encoded_len = 0U;
    cpl.status = status;
}

static doca_error_t submit_batch_via_dpa_ring(GrpcProtoRuntime *rt,
                                              const std::vector<ProtoTask> &tasks,
                                              std::vector<ProtoCompletion> &cpls)
{
    doca_error_t result;
    std::vector<uint32_t> request_map_keys;
    std::vector<uint32_t> request_map_values;
    GrpcShardDispatchPlan plan;
    std::vector<uint32_t> posted_per_shard;
    std::vector<uint32_t> completed_per_shard;
    uint32_t completed_total = 0U;
    bool progress = true;

    if (!grpc_build_shard_dispatch_plan(tasks, rt->shard_count, &plan))
        return DOCA_ERROR_INVALID_VALUE;
    if (!grpc_build_request_index_map(tasks, request_map_keys, request_map_values))
        return DOCA_ERROR_INVALID_VALUE;

    posted_per_shard.assign(rt->shard_count, 0U);
    completed_per_shard.assign(rt->shard_count, 0U);

    for (uint32_t shard_idx = 0; shard_idx < rt->shard_count; ++shard_idx) {
        auto &shard = rt->shards[shard_idx];
        const auto &indices = plan.task_indices_by_shard[shard_idx];
        const uint32_t depth = rt->ring_depth;
        const uint64_t msg_stride = static_cast<uint64_t>(GRPC_DPA_MAX_FLAT_MSG_SIZE);

        for (uint32_t local_idx = 0; local_idx < indices.size(); ++local_idx) {
            uint32_t task_idx = indices[local_idx];
            const ProtoTask &task = tasks[task_idx];
            uint32_t next_tail = (shard.host_ring_ctrl.req_tail + 1U) % depth;
            uint32_t slot;
            GrpcReqDesc *req;

            if (next_tail == shard.host_ring_ctrl.req_head) {
                fill_error_completion_for_task(task, -100, cpls[task_idx]);
                continue;
            }

            slot = shard.host_ring_ctrl.req_tail;
            req = &shard.host_req_ring[slot];
            req->request_id = task.request_id;
            req->desc_id = task.desc_id;
            req->msg_slot = slot;
            req->out_slot = slot;
            req->msg_len = task.host_msg_len;
            req->out_cap = task.dpa_out_cap;
            req->valid = 1U;

            result = doca_dpa_h2d_memcpy(rt->dpa,
                                         add_u64_offset(shard.dpa_msg_pool_addr, (uint64_t)slot * msg_stride),
                                         (void *)task.host_msg_addr,
                                         task.host_msg_len);
            if (result != DOCA_SUCCESS) {
                req->valid = 0U;
                fill_error_completion_for_task(task, -101, cpls[task_idx]);
                continue;
            }

            result = ring_req_sync_h2d(rt, shard, slot);
            if (result != DOCA_SUCCESS) {
                req->valid = 0U;
                fill_error_completion_for_task(task, -102, cpls[task_idx]);
                continue;
            }

            shard.host_ring_ctrl.req_tail = next_tail;
            result = ring_ctrl_sync_h2d(rt, shard);
            if (result != DOCA_SUCCESS) {
                fill_error_completion_for_task(task, -103, cpls[task_idx]);
                continue;
            }

            posted_per_shard[shard_idx]++;
        }
    }

    while (completed_total < tasks.size()) {
        progress = false;

        for (uint32_t shard_idx = 0; shard_idx < rt->shard_count; ++shard_idx) {
            auto &shard = rt->shards[shard_idx];
            const uint64_t out_stride = static_cast<uint64_t>(GRPC_DPA_MAX_ENCODED_BUF_SIZE);

            if (completed_per_shard[shard_idx] >= posted_per_shard[shard_idx])
                continue;

            if (shard.host_ring_ctrl.cpl_head == shard.host_ring_ctrl.cpl_tail) {
                result = doca_dpa_d2h_memcpy(rt->dpa, &shard.host_ring_ctrl, shard.dpa_ring_ctrl_addr, sizeof(shard.host_ring_ctrl));
                if (result != DOCA_SUCCESS)
                    return result;
                continue;
            }

            {
                uint32_t head = shard.host_ring_ctrl.cpl_head;
                GrpcCplDesc *cdesc;
                uint32_t task_idx;

                result = ring_cpl_sync_d2h(rt, shard, head);
                if (result != DOCA_SUCCESS)
                    return result;

                cdesc = &shard.host_cpl_ring[head];
                if (!grpc_lookup_request_index(request_map_keys, request_map_values, cdesc->request_id, &task_idx))
                    return DOCA_ERROR_BAD_STATE;

                cpls[task_idx].request_id = cdesc->request_id;
                cpls[task_idx].encoded_len = cdesc->encoded_len;
                cpls[task_idx].status = cdesc->status;

                if (cpls[task_idx].status == 0) {
                    result = doca_dpa_d2h_memcpy(rt->dpa,
                                                 tasks[task_idx].host_out_addr,
                                                 add_u64_offset(shard.dpa_out_pool_addr, (uint64_t)cdesc->out_slot * out_stride),
                                                 cpls[task_idx].encoded_len);
                    if (result != DOCA_SUCCESS)
                        return result;
                }

                cdesc->valid = 0U;
                result = doca_dpa_h2d_memcpy(rt->dpa,
                                             shard.dpa_cpl_ring_addr + ((uint64_t)head * sizeof(GrpcCplDesc)),
                                             cdesc,
                                             sizeof(*cdesc));
                if (result != DOCA_SUCCESS)
                    return result;

                shard.host_ring_ctrl.cpl_head = (head + 1U) % rt->ring_depth;
                result = ring_ctrl_sync_h2d(rt, shard);
                if (result != DOCA_SUCCESS)
                    return result;

                shard.completions_seen++;
                completed_per_shard[shard_idx]++;
                completed_total++;
                progress = true;
            }
        }

        if (!progress) {
            uint32_t error_count = 0U;

            for (size_t i = 0; i < tasks.size(); ++i) {
                if (cpls[i].status != 0)
                    error_count++;
            }
            if (completed_total + error_count >= tasks.size())
                break;
        }
    }

    return DOCA_SUCCESS;
}
#endif

doca_error_t grpc_proto_runtime_init(GrpcProtoRuntime *rt,
                                     doca_dpa *dpa,
                                     doca_dpa_thread *thread,
                                     uint32_t max_batch)
{
    return grpc_proto_runtime_init_ex(rt, dpa, thread, max_batch, GRPC_DPA_RING_SHARD_COUNT);
}

doca_error_t grpc_proto_runtime_init_ex(GrpcProtoRuntime *rt,
                                        doca_dpa *dpa,
                                        doca_dpa_thread *thread,
                                        uint32_t max_batch,
                                        uint32_t shard_count)
{
    doca_error_t result;
    ProtoDescBlob blob;
    uint64_t msg_total_bytes;
    uint64_t out_total_bytes;
#ifdef GRPC_DPA_ENABLE_RING_LOOP
    uint32_t ring_depth;
#endif

    if (rt == nullptr || dpa == nullptr)
        return DOCA_ERROR_INVALID_VALUE;
    if (max_batch == 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (shard_count == 0U)
        return DOCA_ERROR_INVALID_VALUE;

    *rt = GrpcProtoRuntime{};
    rt->dpa = dpa;
    rt->thread = thread;
    rt->max_batch = max_batch;
    rt->ring_depth = max_batch + 1U;
    rt->shard_count = shard_count;
    rt->selective_policy.string_threshold = GRPC_DPA_SELECTIVE_STRING_THRESHOLD;
    rt->selective_policy.bytes_threshold = GRPC_DPA_SELECTIVE_STRING_THRESHOLD;
    grpc_selective_policy_stats_reset(&rt->selective_policy_stats);
#ifdef GRPC_DPA_ENABLE_RING_LOOP
    ring_depth = rt->ring_depth;
#endif

    msg_total_bytes = static_cast<uint64_t>(GRPC_DPA_MAX_FLAT_MSG_SIZE) * max_batch;
    out_total_bytes = static_cast<uint64_t>(GRPC_DPA_MAX_ENCODED_BUF_SIZE) * max_batch;

    result = doca_dpa_mem_alloc(rt->dpa, sizeof(GrpcDpaWorkerArg), &rt->dpa_worker_arg_addr);
    if (result != DOCA_SUCCESS) goto fail;

    result = doca_dpa_mem_alloc(rt->dpa, sizeof(ProtoDescBlob), &rt->dpa_desc_blob_addr);
    if (result != DOCA_SUCCESS) goto fail;

    result = doca_dpa_mem_alloc(rt->dpa, sizeof(ProtoTask) * max_batch, &rt->dpa_task_array_addr);
    if (result != DOCA_SUCCESS) goto fail;

    result = doca_dpa_mem_alloc(rt->dpa, sizeof(ProtoCompletion) * max_batch, &rt->dpa_completion_array_addr);
    if (result != DOCA_SUCCESS) goto fail;

    result = doca_dpa_mem_alloc(rt->dpa, msg_total_bytes, &rt->dpa_msg_scratch_addr);
    if (result != DOCA_SUCCESS) goto fail;

    result = doca_dpa_mem_alloc(rt->dpa, out_total_bytes, &rt->dpa_out_scratch_addr);
    if (result != DOCA_SUCCESS) goto fail;

#ifdef GRPC_DPA_ENABLE_RING_LOOP
    rt->shards.resize(shard_count);
    for (uint32_t shard_idx = 0; shard_idx < shard_count; ++shard_idx) {
        auto &shard = rt->shards[shard_idx];

        shard.shard_id = shard_idx;
        result = doca_dpa_mem_alloc(rt->dpa, sizeof(GrpcDpaWorkerArg), &shard.dpa_worker_arg_addr);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_mem_alloc(rt->dpa, sizeof(GrpcRingCtrl), &shard.dpa_ring_ctrl_addr);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_mem_alloc(rt->dpa, sizeof(GrpcReqDesc) * ring_depth, &shard.dpa_req_ring_addr);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_mem_alloc(rt->dpa, sizeof(GrpcCplDesc) * ring_depth, &shard.dpa_cpl_ring_addr);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_mem_alloc(rt->dpa, msg_total_bytes, &shard.dpa_msg_pool_addr);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_mem_alloc(rt->dpa, out_total_bytes, &shard.dpa_out_pool_addr);
        if (result != DOCA_SUCCESS) goto fail;
    }
#endif

    rt->host_flat_scratch.resize(static_cast<size_t>(msg_total_bytes));
    rt->host_out_scratch.resize(static_cast<size_t>(out_total_bytes));

    result = grpc_dpa_offload_init(&rt->offload,
                                   rt->dpa,
                                   rt->dpa_worker_arg_addr,
                                   rt->dpa_desc_blob_addr,
                                   rt->dpa_task_array_addr,
                                   rt->dpa_completion_array_addr,
                                   max_batch);
    if (result != DOCA_SUCCESS)
        return result;

    blob = build_desc_blob();
    result = grpc_dpa_push_desc_blob(&rt->offload, &blob);
    if (result != DOCA_SUCCESS) goto fail;

#ifdef GRPC_DPA_ENABLE_RING_LOOP
    for (uint32_t shard_idx = 0; shard_idx < shard_count; ++shard_idx) {
        auto &shard = rt->shards[shard_idx];
        GrpcDpaWorkerArg worker_arg = rt->offload.worker_arg;

        worker_arg.shard_id = shard_idx;
        worker_arg.shard_count = shard_count;
        worker_arg.ring_ctrl_addr = shard.dpa_ring_ctrl_addr;
        worker_arg.req_ring_addr = shard.dpa_req_ring_addr;
        worker_arg.cpl_ring_addr = shard.dpa_cpl_ring_addr;
        worker_arg.msg_pool_addr = shard.dpa_msg_pool_addr;
        worker_arg.out_pool_addr = shard.dpa_out_pool_addr;
        worker_arg.ring_depth = ring_depth;
        worker_arg.msg_slot_size = GRPC_DPA_MAX_FLAT_MSG_SIZE;
        worker_arg.out_slot_size = GRPC_DPA_MAX_ENCODED_BUF_SIZE;

        result = doca_dpa_h2d_memcpy(rt->dpa,
                                     shard.dpa_worker_arg_addr,
                                     &worker_arg,
                                     sizeof(worker_arg));
        if (result != DOCA_SUCCESS) goto fail;

        shard.host_ring_ctrl = GrpcRingCtrl{};
        shard.host_req_ring.assign(ring_depth, GrpcReqDesc{});
        shard.host_cpl_ring.assign(ring_depth, GrpcCplDesc{});
        result = ring_ctrl_sync_h2d(rt, shard);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_h2d_memcpy(rt->dpa, shard.dpa_req_ring_addr, shard.host_req_ring.data(), sizeof(GrpcReqDesc) * ring_depth);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_h2d_memcpy(rt->dpa, shard.dpa_cpl_ring_addr, shard.host_cpl_ring.data(), sizeof(GrpcCplDesc) * ring_depth);
        if (result != DOCA_SUCCESS) goto fail;

        result = doca_dpa_thread_create(rt->dpa, &shard.thread);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_thread_set_func_arg(shard.thread, grpc_dpa_worker_main, shard.dpa_worker_arg_addr);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_thread_start(shard.thread);
        if (result != DOCA_SUCCESS) goto fail;
        result = doca_dpa_thread_run(shard.thread);
        if (result != DOCA_SUCCESS) goto fail;
    }
#endif

    return DOCA_SUCCESS;

fail:
    grpc_proto_runtime_destroy(rt);
    return result;
}

void grpc_proto_runtime_destroy(GrpcProtoRuntime *rt)
{
    if (rt == nullptr || rt->dpa == nullptr)
        return;

#ifdef GRPC_DPA_ENABLE_RING_LOOP
    for (auto &shard : rt->shards) {
        if (shard.dpa_ring_ctrl_addr != 0) {
            shard.host_ring_ctrl.shutdown = 1U;
            (void)ring_ctrl_sync_h2d(rt, shard);
        }
    }
    for (auto &shard : rt->shards) {
        if (shard.thread != nullptr) {
            (void)doca_dpa_thread_stop(shard.thread);
            (void)doca_dpa_thread_destroy(shard.thread);
            shard.thread = nullptr;
        }
    }
    for (auto &shard : rt->shards) {
        if (shard.dpa_out_pool_addr != 0)
            (void)doca_dpa_mem_free(rt->dpa, shard.dpa_out_pool_addr);
        if (shard.dpa_msg_pool_addr != 0)
            (void)doca_dpa_mem_free(rt->dpa, shard.dpa_msg_pool_addr);
        if (shard.dpa_cpl_ring_addr != 0)
            (void)doca_dpa_mem_free(rt->dpa, shard.dpa_cpl_ring_addr);
        if (shard.dpa_req_ring_addr != 0)
            (void)doca_dpa_mem_free(rt->dpa, shard.dpa_req_ring_addr);
        if (shard.dpa_ring_ctrl_addr != 0)
            (void)doca_dpa_mem_free(rt->dpa, shard.dpa_ring_ctrl_addr);
        if (shard.dpa_worker_arg_addr != 0)
            (void)doca_dpa_mem_free(rt->dpa, shard.dpa_worker_arg_addr);
    }
#endif

    if (rt->dpa_out_scratch_addr != 0)
        (void)doca_dpa_mem_free(rt->dpa, rt->dpa_out_scratch_addr);
    if (rt->dpa_msg_scratch_addr != 0)
        (void)doca_dpa_mem_free(rt->dpa, rt->dpa_msg_scratch_addr);
    if (rt->dpa_completion_array_addr != 0)
        (void)doca_dpa_mem_free(rt->dpa, rt->dpa_completion_array_addr);
    if (rt->dpa_task_array_addr != 0)
        (void)doca_dpa_mem_free(rt->dpa, rt->dpa_task_array_addr);
    if (rt->dpa_desc_blob_addr != 0)
        (void)doca_dpa_mem_free(rt->dpa, rt->dpa_desc_blob_addr);
    if (rt->dpa_worker_arg_addr != 0)
        (void)doca_dpa_mem_free(rt->dpa, rt->dpa_worker_arg_addr);

    rt->dpa_out_scratch_addr = 0;
    rt->dpa_msg_scratch_addr = 0;
    rt->dpa_completion_array_addr = 0;
    rt->dpa_task_array_addr = 0;
    rt->dpa_desc_blob_addr = 0;
    rt->dpa_worker_arg_addr = 0;
    rt->host_flat_scratch.clear();
    rt->host_out_scratch.clear();
    rt->host_req_ring.clear();
    rt->host_cpl_ring.clear();
#ifdef GRPC_DPA_ENABLE_RING_LOOP
    rt->shards.clear();
#endif
}

doca_error_t grpc_proto_serialize_hello(GrpcProtoRuntime *rt,
                                        const demo::HelloRequest &req,
                                        uint32_t request_id,
                                        std::vector<uint8_t> &encoded,
                                        ProtoCompletion *cpl_out)
{
    std::vector<demo::HelloRequest> reqs(1, req);
    std::vector<uint32_t> request_ids(1, request_id);
    std::vector<std::vector<uint8_t>> encoded_batch;
    std::vector<ProtoCompletion> cpls;
    doca_error_t result;

    if (rt == nullptr || rt->max_batch == 0)
        return DOCA_ERROR_INVALID_VALUE;

    result = grpc_proto_serialize_hello_batch(rt, reqs, request_ids, encoded_batch, &cpls);
    if (result != DOCA_SUCCESS)
        return result;

    encoded = std::move(encoded_batch[0]);

    if (cpl_out != nullptr && !cpls.empty())
        *cpl_out = cpls[0];

    return DOCA_SUCCESS;
}

doca_error_t grpc_proto_serialize_hello_batch(GrpcProtoRuntime *rt,
                                              const std::vector<demo::HelloRequest> &reqs,
                                              const std::vector<uint32_t> &request_ids,
                                              std::vector<std::vector<uint8_t>> &encoded_batch,
                                              std::vector<ProtoCompletion> *cpls_out)
{
    std::vector<ProtoTask> tasks;
    std::vector<ProtoCompletion> cpls;
    doca_error_t result;

    if (rt == nullptr || rt->max_batch == 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (reqs.empty())
        return DOCA_ERROR_INVALID_VALUE;
    if (request_ids.size() != reqs.size())
        return DOCA_ERROR_INVALID_VALUE;
    if (reqs.size() > rt->max_batch)
        return DOCA_ERROR_INVALID_VALUE;
    if (rt->host_flat_scratch.size() < reqs.size() * GRPC_DPA_MAX_FLAT_MSG_SIZE)
        return DOCA_ERROR_BAD_STATE;
    if (rt->host_out_scratch.size() < reqs.size() * GRPC_DPA_MAX_ENCODED_BUF_SIZE)
        return DOCA_ERROR_BAD_STATE;

    result = prepare_batch_tasks(rt, reqs, request_ids, tasks);
    if (result != DOCA_SUCCESS)
        return result;

    cpls.resize(reqs.size());

#ifdef GRPC_DPA_ENABLE_RING_LOOP
    result = submit_batch_via_dpa_ring(rt, tasks, cpls);
    if (result != DOCA_SUCCESS)
        return result;
#else
    result = grpc_dpa_submit_batch(&rt->offload, tasks.data(), static_cast<uint32_t>(tasks.size()), cpls.data());
    if (result != DOCA_SUCCESS)
        return result;
#endif

    encoded_batch.resize(reqs.size());
    for (size_t i = 0; i < reqs.size(); ++i) {
        if (cpls[i].status == 0) {
            encoded_batch[i].assign(rt->host_out_scratch.data() + i * GRPC_DPA_MAX_ENCODED_BUF_SIZE,
                                    rt->host_out_scratch.data() + i * GRPC_DPA_MAX_ENCODED_BUF_SIZE + cpls[i].encoded_len);
        } else {
            encoded_batch[i].clear();
        }
    }

    if (cpls_out != nullptr)
        *cpls_out = std::move(cpls);

    return DOCA_SUCCESS;
}

#ifdef GRPC_DPA_STANDALONE_MAIN
#include <iostream>

int main()
{
    std::cerr << "Build with existing DPUMesh DOCA runtime and call grpc_proto_runtime_init() explicitly.\n";
    return 1;
}
#endif
