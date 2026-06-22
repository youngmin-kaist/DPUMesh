#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"
#include "grpc_wire_encode.h"

#include <stddef.h>

#define DMESH_GRPC_SERIALIZE_MODE_GRPC 0U
#define DMESH_GRPC_SERIALIZE_MODE_GRPC_REVERSE 1U
#define DMESH_GRPC_SERIALIZE_MODE_COPY 2U
#define DMESH_GRPC_SERIALIZE_MODE_DMA_COPY 3U

#ifndef DMESH_GRPC_SERIALIZE_DEFAULT_MODE
#define DMESH_GRPC_SERIALIZE_DEFAULT_MODE DMESH_GRPC_SERIALIZE_MODE_GRPC_REVERSE
#endif

#define POLL_GRPC 1

/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @return: returns RPC_RETURN_STATUS_SUCCESS on success and RPC_RETURN_STATUS_ERROR otherwise
 */
__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer,
                                     uint32_t num_msg,
                                     doca_dpa_dev_notification_completion_t main_notify)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);
    if (main_notify != 0) {
        doca_dpa_dev_thread_notify(main_notify);
    }

    return 0;
}

/*
 * The current runtime starts one DMA-manager DPA thread. DOCA DMA send may keep
 * the immediate-data pointer until completion, so outstanding requests cannot
 * use stack metadata. Make this table per-thread before running multiple DMA
 * managers in one DPA process.
 */
static struct dpa_grpc_serialize_task grpc_tasks[DMESH_GRPC_MAX_PENDING];
static struct comch_dma_comp_msg grpc_copy_dma_comp_imm[DMESH_GRPC_MAX_PENDING];

#define DMESH_GRPC_STALL_LOG_POLL_INTERVAL 65536U

static const ProtoSchemaBlob grpc_schema_blob = {
    .msg_count = 1U,
    .field_count = 3U,
    .msgs = {
        {
            .schema_id = DMESH_GRPC_SCHEMA_HELLO_REQUEST,
            .field_begin = 0U,
            .field_count = 3U,
            .flat_size = sizeof(HelloRequestFlat),
        },
    },
    .fields = {
        {
            .field_no = 1U,
            .kind = FK_U64,
            .offset = (uint32_t)offsetof(HelloRequestFlat, id),
            .child_schema_id = 0U,
        },
        {
            .field_no = 2U,
            .kind = FK_STRING,
            .offset = (uint32_t)offsetof(HelloRequestFlat, name),
            .child_schema_id = 0U,
        },
        {
            .field_no = 3U,
            .kind = FK_REPEATED_U32_PACKED,
            .offset = (uint32_t)offsetof(HelloRequestFlat, scores),
            .child_schema_id = 0U,
        },
    },
};

struct grpc_dpa_copy_ctx {
    struct dpa_thread_arg *thread_arg;
    uint32_t done_idx;
    uint8_t from_host;
};

static inline doca_dpa_dev_uintptr_t 
get_dev_ptr(doca_dpa_dev_buf_arr_t buf_arr, uint32_t index)
{
    doca_dpa_dev_buf_t buf;

    buf = doca_dpa_dev_buf_array_get_buf(buf_arr, index);
    return doca_dpa_dev_buf_get_external_ptr(buf);
}

static doca_dpa_dev_uintptr_t 
dpa_mmap_ptr(doca_dpa_dev_buf_arr_t buf_arr,
                             uint64_t base_addr,
                             uint32_t buf_size,
                             uint64_t addr,
                             uint64_t size)
{
    uint64_t offset = addr - base_addr;

    if ((addr < base_addr) || (offset + size > buf_size)) {
        return 0;
    }

    return get_dev_ptr(buf_arr, 0) + offset;
}

static doca_dpa_dev_uintptr_t 
dpa_dpu_ptr(struct dpa_thread_arg *thread_arg, uint64_t addr, uint64_t size)
{
    return dpa_mmap_ptr(thread_arg->dpa_dpu_mmap_buf_arr,
                        thread_arg->dpu_base_addr,
                        thread_arg->buf_size,
                        addr,
                        size);
}

static inline struct dpa_grpc_pipeline_state 
*grpc_pipeline_state(struct dpa_thread_arg *thread_arg)
{
    return (struct dpa_grpc_pipeline_state *)(uintptr_t)thread_arg->pipeline_state;
}

#if DMESH_GRPC_PIPELINE_PROFILE
/*
 * Profiling counters are observational only. They intentionally use relaxed
 * atomics and must not be treated as task ownership/order synchronization; the
 * queue pointer/prod/cons release-acquire protocol remains authoritative.
 */
static inline uint64_t
grpc_profile_now_us(void)
{
    return __dpa_thread_time();
}

static inline uint64_t
grpc_profile_load_u64(const uint64_t *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_RELAXED);
}

static inline void
grpc_profile_store_u64(uint64_t *ptr, uint64_t value)
{
    __atomic_store_n(ptr, value, __ATOMIC_RELAXED);
}

static inline void
grpc_profile_add_u64(uint64_t *ptr, uint64_t delta)
{
    uint64_t value = grpc_profile_load_u64(ptr);

    grpc_profile_store_u64(ptr, value + delta);
}

static inline uint64_t
grpc_profile_per_sec(uint64_t count, uint64_t window_us)
{
    if (window_us == 0U)
        return 0U;

    return (count * 1000000ULL) / window_us;
}

static inline void
grpc_profile_note_serializer_enqueue(struct dpa_grpc_pipeline_state *state,
                                     uint32_t worker,
                                     uint32_t occupancy)
{
    uint32_t max_occupancy;

    if (state == 0 || worker >= DMESH_GRPC_SERIALIZER_THREADS)
        return;

    max_occupancy = __atomic_load_n(&state->profile.serializer_queue_max_occupancy[worker],
                                    __ATOMIC_RELAXED);
    if (occupancy > max_occupancy) {
        __atomic_store_n(&state->profile.serializer_queue_max_occupancy[worker],
                         occupancy,
                         __ATOMIC_RELAXED);
    }
}

static inline void
grpc_profile_note_serializer_completed(struct dpa_grpc_pipeline_state *state,
                                       uint32_t worker)
{
    if (state == 0 || worker >= DMESH_GRPC_SERIALIZER_THREADS)
        return;

    grpc_profile_add_u64(&state->profile.serializer_completed[worker], 1U);
}

static inline void
grpc_profile_note_serializer_idle_poll(struct dpa_grpc_pipeline_state *state,
                                       uint32_t worker)
{
    if (state == 0 || worker >= DMESH_GRPC_SERIALIZER_THREADS)
        return;

    grpc_profile_add_u64(&state->profile.serializer_idle_polls[worker], 1U);
}

static inline void
grpc_profile_note_serializer_reschedule(struct dpa_grpc_pipeline_state *state,
                                        uint32_t worker)
{
    if (state == 0 || worker >= DMESH_GRPC_SERIALIZER_THREADS)
        return;

    grpc_profile_add_u64(&state->profile.serializer_reschedules[worker], 1U);
}

static inline void
grpc_profile_note_retire_stall(struct dpa_grpc_pipeline_state *state,
                               uint8_t new_stall_event)
{
    if (state == 0)
        return;

    grpc_profile_add_u64(&state->profile.retire_stall_polls, 1U);
    if (new_stall_event)
        grpc_profile_add_u64(&state->profile.retire_stall_events, 1U);
}

static void
grpc_profile_report_dispatcher(struct dpa_thread_arg *thread_arg,
                               uint32_t dispatched)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    struct dpa_grpc_pipeline_profile *profile;
    uint64_t total_dispatched;
    uint64_t last_dispatched;
    uint64_t dispatch_delta;
    uint64_t now_us;
    uint64_t last_us;
    uint64_t window_us;
    uint64_t retire_stall_polls;
    uint64_t retire_stall_events;
    uint64_t retire_stall_polls_delta;
    uint64_t retire_stall_events_delta;
    uint32_t dispatch_prod;
    uint32_t dispatch_cons;
    uint32_t dispatch_occupancy;
    uint32_t i;

    if (state == 0 || dispatched == 0U)
        return;

    profile = &state->profile;
    total_dispatched = grpc_profile_load_u64(&profile->dispatcher_dispatched) + dispatched;
    grpc_profile_store_u64(&profile->dispatcher_dispatched, total_dispatched);

    last_dispatched = grpc_profile_load_u64(&profile->dispatcher_last_dispatched);
    dispatch_delta = total_dispatched - last_dispatched;
    if (dispatch_delta < DMESH_GRPC_PROFILE_LOG_INTERVAL)
        return;

    now_us = grpc_profile_now_us();
    last_us = grpc_profile_load_u64(&profile->dispatcher_last_report_us);
    if (last_us == 0U) {
        grpc_profile_store_u64(&profile->dispatcher_last_report_us, now_us);
        grpc_profile_store_u64(&profile->dispatcher_last_dispatched, total_dispatched);
        return;
    }
    window_us = now_us - last_us;

    retire_stall_polls = grpc_profile_load_u64(&profile->retire_stall_polls);
    retire_stall_events = grpc_profile_load_u64(&profile->retire_stall_events);
    retire_stall_polls_delta =
        retire_stall_polls - grpc_profile_load_u64(&profile->retire_last_stall_polls);
    retire_stall_events_delta =
        retire_stall_events - grpc_profile_load_u64(&profile->retire_last_stall_events);

    dispatch_prod = __atomic_load_n(&state->dispatch_prod, __ATOMIC_ACQUIRE);
    dispatch_cons = __atomic_load_n(&state->dispatch_cons, __ATOMIC_ACQUIRE);
    dispatch_occupancy = dispatch_prod - dispatch_cons;

    DOCA_DPA_DEV_LOG_INFO("gRPC profile dispatcher: serializers=%u dispatched_total=%lu "
                          "dispatched_delta=%lu dispatched_per_sec=%lu window_us=%lu "
                          "dispatch_q_occ=%u retire_stall_polls_delta=%lu "
                          "retire_stall_events_delta=%lu retire_stall_polls_total=%lu "
                          "retire_stall_events_total=%lu\n",
                          DMESH_GRPC_SERIALIZER_THREADS,
                          (unsigned long)total_dispatched,
                          (unsigned long)dispatch_delta,
                          (unsigned long)grpc_profile_per_sec(dispatch_delta, window_us),
                          (unsigned long)window_us,
                          dispatch_occupancy,
                          (unsigned long)retire_stall_polls_delta,
                          (unsigned long)retire_stall_events_delta,
                          (unsigned long)retire_stall_polls,
                          (unsigned long)retire_stall_events);

    for (i = 0; i < DMESH_GRPC_SERIALIZER_THREADS; ++i) {
        uint64_t completed = grpc_profile_load_u64(&profile->serializer_completed[i]);
        uint64_t idle_polls = grpc_profile_load_u64(&profile->serializer_idle_polls[i]);
        uint64_t reschedules = grpc_profile_load_u64(&profile->serializer_reschedules[i]);
        uint64_t completed_delta =
            completed - grpc_profile_load_u64(&profile->serializer_last_completed[i]);
        uint64_t idle_polls_delta =
            idle_polls - grpc_profile_load_u64(&profile->serializer_last_idle_polls[i]);
        uint64_t reschedules_delta =
            reschedules - grpc_profile_load_u64(&profile->serializer_last_reschedules[i]);
        uint32_t prod = __atomic_load_n(&state->serializer_prod[i], __ATOMIC_ACQUIRE);
        uint32_t cons = __atomic_load_n(&state->serializer_cons[i], __ATOMIC_ACQUIRE);
        uint32_t occupancy = prod - cons;
        uint32_t max_occupancy =
            __atomic_load_n(&profile->serializer_queue_max_occupancy[i],
                            __ATOMIC_RELAXED);

        DOCA_DPA_DEV_LOG_INFO("gRPC profile worker: serializers=%u worker=%u "
                              "completed_delta=%lu completed_per_sec=%lu "
                              "completed_total=%lu idle_polls_delta=%lu "
                              "idle_polls_total=%lu reschedules_delta=%lu "
                              "reschedules_total=%lu q_occ=%u q_max=%u prod=%u cons=%u\n",
                              DMESH_GRPC_SERIALIZER_THREADS,
                              i,
                              (unsigned long)completed_delta,
                              (unsigned long)grpc_profile_per_sec(completed_delta, window_us),
                              (unsigned long)completed,
                              (unsigned long)idle_polls_delta,
                              (unsigned long)idle_polls,
                              (unsigned long)reschedules_delta,
                              (unsigned long)reschedules,
                              occupancy,
                              max_occupancy,
                              prod,
                              cons);

        grpc_profile_store_u64(&profile->serializer_last_completed[i], completed);
        grpc_profile_store_u64(&profile->serializer_last_idle_polls[i], idle_polls);
        grpc_profile_store_u64(&profile->serializer_last_reschedules[i], reschedules);
        __atomic_store_n(&profile->serializer_queue_max_occupancy[i],
                         occupancy,
                         __ATOMIC_RELAXED);
    }

    grpc_profile_store_u64(&profile->dispatcher_last_dispatched, total_dispatched);
    grpc_profile_store_u64(&profile->dispatcher_last_report_us, now_us);
    grpc_profile_store_u64(&profile->retire_last_stall_polls, retire_stall_polls);
    grpc_profile_store_u64(&profile->retire_last_stall_events, retire_stall_events);
}
#endif

static uint32_t
drain_dpa_producer_completions(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_element_t comp;
    uint32_t drained = 0;

    while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) != 0) {
        drained++;
    }

    if (drained != 0) {
        doca_dpa_dev_completion_ack(thread_arg->dpa_producer_comp, drained);
    }

    return drained;
}

static void 
wait_for_dpu_consumer(struct dpa_thread_arg *thread_arg)
{
    while (doca_dpa_dev_comch_producer_is_consumer_empty(
          thread_arg->dpa_producer, /*consumer_id=*/1) != 0) {
        drain_dpa_producer_completions(thread_arg);
    }
}

static uint32_t
drain_dpa_copy_completions(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_element_t comp;
    uint32_t drained = 0;

    if (thread_arg->dpa_copy_comp == 0)
        return 0;

    while (doca_dpa_dev_get_completion(thread_arg->dpa_copy_comp, &comp) != 0) {
        drained++;
    }

    if (drained != 0) {
        doca_dpa_dev_completion_ack(thread_arg->dpa_copy_comp, drained);
    }

    return drained;
}

static int
wait_for_dpa_copy_completion(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_element_t comp;
    doca_dpa_dev_completion_type_t type;

    if (thread_arg->dpa_copy_comp == 0)
        return -5;

    while (doca_dpa_dev_get_completion(thread_arg->dpa_copy_comp, &comp) == 0) {
        __dpa_thread_window_read_inv();
    }

    type = doca_dpa_dev_get_completion_type(comp);
    doca_dpa_dev_completion_ack(thread_arg->dpa_copy_comp, 1);

    if (type == DOCA_DPA_DEV_COMP_SEND_ERR || type == DOCA_DPA_DEV_COMP_RECV_ERR)
        return -5;

    return 0;
}

static int
wait_for_dpa_producer_completion(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_element_t comp;
    doca_dpa_dev_completion_type_t type;

    if (thread_arg->dpa_producer_comp == 0)
        return -5;

    while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) == 0) {
        __dpa_thread_window_read_inv();
    }

    type = doca_dpa_dev_get_completion_type(comp);
    doca_dpa_dev_completion_ack(thread_arg->dpa_producer_comp, 1);

    if (type == DOCA_DPA_DEV_COMP_SEND_ERR || type == DOCA_DPA_DEV_COMP_RECV_ERR)
        return -5;

    return 0;
}

static int
submit_grpc_post_copy(void *opaque,
                      uint64_t dst_addr,
                      uint64_t src_addr,
                      uint32_t len)
{
    struct grpc_dpa_copy_ctx *ctx = (struct grpc_dpa_copy_ctx *)opaque;
    struct dpa_thread_arg *thread_arg;
    doca_dpa_dev_mmap_t src_mmap;

    if (ctx == 0 || ctx->thread_arg == 0)
        return -1;
        
    if (len == 0U)
        return 0;

    thread_arg = ctx->thread_arg;
    if (thread_arg->dpa_copy_async_ops == 0 || thread_arg->dpa_copy_comp == 0)
        return -5;

    drain_dpa_copy_completions(thread_arg);

    if (ctx->from_host) {
        src_mmap = thread_arg->host_mmap;
    } else {
        src_mmap = thread_arg->dpu_mmap;
    }

    doca_dpa_dev_post_memcpy(thread_arg->dpa_copy_async_ops,
                            thread_arg->dpu_mmap,
                            dst_addr,
                            src_mmap,
                            src_addr,
                            len,
                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    return wait_for_dpa_copy_completion(thread_arg);
}

static int
submit_grpc_host_to_dpu_copy(struct dpa_thread_arg *thread_arg,
                             uint64_t dst_addr,
                             uint64_t src_addr,
                             uint32_t len)
{
    if (thread_arg->dpa_copy_async_ops == 0 || thread_arg->dpa_copy_comp == 0)
        return -5;

    drain_dpa_copy_completions(thread_arg);
    doca_dpa_dev_post_memcpy(thread_arg->dpa_copy_async_ops,
                             thread_arg->dpu_mmap,
                             dst_addr,
                             thread_arg->host_mmap,
                             src_addr,
                             len,
                             DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
                            //  DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);

    return wait_for_dpa_copy_completion(thread_arg);
}

static int
submit_grpc_dma_copy(void *opaque,
                     uint64_t dst_addr,
                     uint64_t src_addr,
                     uint32_t len)
{
    struct grpc_dpa_copy_ctx *ctx = (struct grpc_dpa_copy_ctx *)opaque;
    struct dpa_thread_arg *thread_arg;
    struct comch_dma_comp_msg *msg;
    doca_dpa_dev_mmap_t src_mmap;

    if (ctx == 0 || ctx->thread_arg == 0)
        return -1;
        
    if (len == 0U)
        return 0;

    thread_arg = ctx->thread_arg;

    msg = &grpc_copy_dma_comp_imm[ctx->done_idx];
    msg->type = COMCH_MSG_TYPE_DMA_COMPLETED;
    msg->pos = 0U;
    msg->length = len;
    msg->idx = ctx->done_idx;
    __sync_synchronize();

    wait_for_dpu_consumer(thread_arg);

    if (ctx->from_host) {
        src_mmap = thread_arg->host_mmap;
    } else {
        src_mmap = thread_arg->dpu_mmap;
    }

    /*
     * Production DMA mode: each serializer thread receives its own Comch
     * producer and producer completion in dpa_thread_arg. The thread must only
     * poll/ack completions generated by that producer; otherwise one
     * serializer can consume another serializer's DMA completion and stall it.
     */
    doca_dpa_dev_comch_producer_dma_copy(thread_arg->dpa_producer,
                                         /*consumer_id=*/1,
                                         thread_arg->dpu_mmap,
                                         dst_addr,
                                         src_mmap,
                                         src_addr,
                                         len,
                                         (uint8_t *)msg,
                                         sizeof(*msg),
                                         DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    return wait_for_dpa_producer_completion(thread_arg);
}

static inline uint32_t 
grpc_serialize_task_cost(uint32_t len)
{
    uint32_t cost = len;
    if (cost == 0U) {
        cost = 1U;

    } else if (cost > DMESH_GRPC_SERIALIZER_DRR_MAX_DEFICIT) {
        cost = DMESH_GRPC_SERIALIZER_DRR_MAX_DEFICIT;
    }

    return cost;
}

static inline uint32_t
drr_add_quantum(uint32_t deficit)
{
    deficit += DMESH_GRPC_SERIALIZER_DRR_QUANTUM;
    if (deficit > DMESH_GRPC_SERIALIZER_DRR_MAX_DEFICIT) {
        deficit = DMESH_GRPC_SERIALIZER_DRR_MAX_DEFICIT;
    }

    return deficit;
}

static int 
schedule_grpc_serialize_task_fields(struct dpa_thread_arg *thread_arg,
                                    struct dpa_grpc_serialize_task *task)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    uint32_t worker = state->serializer_drr_cursor % DMESH_GRPC_SERIALIZER_THREADS;
    uint32_t cost = grpc_serialize_task_cost(task->len);
    uint32_t deficit = state->serializer_drr_deficit[worker];
    uint32_t prod;
    uint32_t cons;

    // find a worker with enough deficit to run the task, using a DRR-like approach
    while (deficit < cost) {
        deficit = drr_add_quantum(deficit);
        state->serializer_drr_deficit[worker] = deficit;
        
        worker = (worker + 1U) % DMESH_GRPC_SERIALIZER_THREADS;
        state->serializer_drr_cursor = worker;
        deficit = state->serializer_drr_deficit[worker];
    }
        
    // wait for an empty slot in the selected worker's queue
    for (;;) {
        prod = __atomic_load_n(&state->serializer_prod[worker], __ATOMIC_ACQUIRE);
        cons = __atomic_load_n(&state->serializer_cons[worker], __ATOMIC_ACQUIRE);

        if (prod - cons < DMESH_GRPC_SERIALIZER_QUEUE_DEPTH)
            break;
    }

    // publish the task to the selected worker's queue
    uint32_t ser_queue_slot = prod % DMESH_GRPC_SERIALIZER_QUEUE_DEPTH;
    if (__atomic_load_n(&state->serializer_tasks[worker][ser_queue_slot], __ATOMIC_ACQUIRE) != NULL) {
        DOCA_DPA_DEV_LOG_INFO("[%u] Unexpected valid task in serializer queue: slot=%u prod=%u cons=%u\n",
                                worker, ser_queue_slot, prod, cons);
        return -1;
    }

    uint32_t dispatch_queue_slot = task->slot_idx;
    enum pipeline_task_state cur_state = 
        __atomic_load_n(&state->pipeline_task_state[dispatch_queue_slot], __ATOMIC_ACQUIRE);

    if (cur_state != TASK_STATE_PROCESSING) {
        DOCA_DPA_DEV_LOG_INFO("[%u] Invalid pipeline task state for gRPC serialize request: req=%u seq=%lu slot=%u state=%u\n",
                                worker, task->request_id, 
                                task->ring_seq, dispatch_queue_slot, cur_state);
        __atomic_store_n(&state->pipeline_task_state[dispatch_queue_slot], 
                         TASK_STATE_ERROR, __ATOMIC_RELEASE);
        return -1;
    }

    #if DEBUG_LOG    
    if (task->ring_seq <= 8U || (task->ring_seq % DEBUG_INTERVAL) == 0U) {
        DOCA_DPA_DEV_LOG_INFO("[%u] Enqueued gRPC serialize task: "
            "req=%u seq=%lu slot=%u len=%u flags=0x%x cost=%u deficit=%u\n",
            worker, task->request_id, task->ring_seq, task->slot_idx,
            task->len, task->flags,
            cost, deficit);
        }
        #endif
        
        /*
        * The dispatcher thread is the single producer for serializer queues.
        * valid/prod are release-published; each serializer worker consumes only
        * its own queue with acquire loads.
        */
    __atomic_store_n(&state->serializer_tasks[worker][ser_queue_slot], task, __ATOMIC_RELEASE);
    __sync_synchronize();
    __atomic_store_n(&state->serializer_prod[worker], prod + 1U, __ATOMIC_RELEASE);
#if DMESH_GRPC_PIPELINE_PROFILE
    grpc_profile_note_serializer_enqueue(state, worker, (prod + 1U) - cons);
#endif

    state->serializer_drr_deficit[worker] = deficit - cost;
    state->serializer_drr_cursor = (worker + 1U) % DMESH_GRPC_SERIALIZER_THREADS;
    
    // notify the serializer worker thread
    if (thread_arg->serializer_notify[worker] != 0)
        doca_dpa_dev_thread_notify(thread_arg->serializer_notify[worker]);
    
    return 0;
}


// DPU -> dispatcher serialize req
static int 
enqueue_grpc_serialize_task(struct dpa_thread_arg *thread_arg,
                                       const struct comch_grpc_serialize_req_msg *req)
{
    struct dpa_grpc_serialize_task task = {0};

    if (req == 0 || req->ring_seq == 0)
        return -1;

    task.slot_idx = (uint32_t)((req->ring_seq - 1U) % DMESH_GRPC_MAX_PENDING);
    task.request_id = req->request_id;
    task.ring_seq = req->ring_seq;
    task.src_addr = 0; // no need to copy from serializer 
    task.len = req->len;
    task.schema_id = req->schema_id;
    task.flags = 0; // no need to copy from serializer

    return schedule_grpc_serialize_task_fields(thread_arg, &task);
}

// main -> dispatcher
static int
enqueue_grpc_dispatch_task_fields(struct dpa_thread_arg *thread_arg,
                                  struct dpa_grpc_serialize_task *task)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    uint32_t prod;
    uint32_t cons;
    uint32_t dispatch_slot;
    uint8_t notify_dispatcher;

    /*
     * Main is the only producer and dispatcher is the only consumer for this
     * queue. If another descriptor producer is added, dispatch_prod needs MPSC
     * protection.
     */
    for (;;) {
        prod = __atomic_load_n(&state->dispatch_prod, __ATOMIC_ACQUIRE);
        cons = __atomic_load_n(&state->dispatch_cons, __ATOMIC_ACQUIRE);
        if (prod - cons < DMESH_GRPC_DISPATCH_QUEUE_DEPTH)
            break;
        __dpa_thread_window_read_inv();
    }

    dispatch_slot = prod % DMESH_GRPC_DISPATCH_QUEUE_DEPTH;
    if (__atomic_load_n(&state->dispatch_tasks[dispatch_slot], __ATOMIC_ACQUIRE) != NULL)
        return -1;
    notify_dispatcher = (prod == cons);

    __atomic_store_n(&state->dispatch_tasks[dispatch_slot], task, __ATOMIC_RELEASE);
    __atomic_store_n(&state->dispatch_prod, prod + 1U, __ATOMIC_RELEASE);
    if (notify_dispatcher && thread_arg->dispatcher_notify != 0) {
        /*
         * Main is the sole dispatch-queue producer. Wake the dispatcher only
         * for the empty -> non-empty transition; after wakeup it owns progress
         * by polling this queue on its dedicated EU.
         */
        doca_dpa_dev_thread_notify(thread_arg->dispatcher_notify);
    }

    return 0;
}

static int 
handle_dpu_msg(struct dpa_thread_arg *thread_arg,
                          const void *msg_data)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    enum comch_msg_type msg_type = *(const enum comch_msg_type *)msg_data;
    const char *imm_msg = "DPU received msg: ";

    switch(msg_type) {
        case COMCH_MSG_TYPE_DMA_REQ: {
            const struct comch_dma_req_msg *dma_msg = (const struct comch_dma_req_msg *)msg_data;
            if (dma_msg->dpa_producer)
                producer = dma_msg->dpa_producer;
            DOCA_DPA_DEV_LOG_INFO("Received DMA REQ msg from host: producer=0x%lx, src_mmap=%u, "
                                    "dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u\n",
                                    producer,                 
                                    dma_msg->src_mmap,
                                    dma_msg->dst_mmap,
                                    dma_msg->src_addr,
                                    dma_msg->dst_addr,
                                    dma_msg->length);
                
            if (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1)) {
                DOCA_DPA_DEV_LOG_INFO("Host consumer is empty, cannot send DMA completion\n");
                break;
            }

            doca_dpa_dev_comch_producer_dma_copy(producer,
                                    /*consumer_id=*/1,
                                    dma_msg->dst_mmap,
                                    dma_msg->dst_addr,
                                    dma_msg->src_mmap,
                                    dma_msg->src_addr,
                                    dma_msg->length,
                                    (uint8_t *)imm_msg,
                                    sizeof(*imm_msg),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            break;
        }
        case COMCH_MSG_TYPE_GRPC_SERIALIZE_REQ: {
            const struct comch_grpc_serialize_req_msg *req =
                (const struct comch_grpc_serialize_req_msg *)msg_data;

            if (enqueue_grpc_serialize_task(thread_arg, req) < 0) {
                // DOCA_DPA_DEV_LOG_INFO("Failed to enqueue gRPC serialize task: req=%u seq=%lu\n",
                //                       req->request_id, req->ring_seq);
                break;
            }
            return 1;
        }
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg_type);
            break;
    }

    return 0;
}

static int 
handle_msgs(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_consumer_completion_element_t completion;
    doca_dpa_dev_comch_consumer_t consumer = thread_arg->dpa_consumer;
	doca_dpa_dev_comch_consumer_completion_t consumer_comp = thread_arg->dpa_consumer_comp;
    int serialized = 0;
    uint32_t num_msgs = 0;
    const uint8_t *msg;
    uint32_t msg_size;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        if (msg_size >= sizeof(enum comch_msg_type) &&
            handle_dpu_msg(thread_arg, msg) > 0) {
            serialized = 1;
        }
        num_msgs++;


        if (num_msgs >= DMESH_GRPC_SERIALIZER_QUEUE_DEPTH) {
            doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		    doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		    doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
            num_msgs = 0;
        }
    }

    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }

    return serialized;
}

static uint32_t
dispatch_grpc_serialize_tasks(struct dpa_thread_arg *thread_arg,
                              uint32_t budget)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    struct dpa_grpc_serialize_task *task;
    uint32_t dispatched = 0;
    uint32_t cons;
    uint32_t prod;
    uint32_t slot;
    int result;

    if (budget == 0)
        budget = DMESH_GRPC_MAX_PENDING;

    while (dispatched < budget) {
        cons = __atomic_load_n(&state->dispatch_cons, __ATOMIC_ACQUIRE);
        prod = __atomic_load_n(&state->dispatch_prod, __ATOMIC_ACQUIRE);
        if (cons == prod)
            break;

        slot = cons % DMESH_GRPC_DISPATCH_QUEUE_DEPTH;
        if (__atomic_load_n(&state->dispatch_tasks[slot], __ATOMIC_ACQUIRE) == NULL) {
            __dpa_thread_window_read_inv();
            continue;
        }
        
        task = state->dispatch_tasks[slot];
        result = schedule_grpc_serialize_task_fields(thread_arg, task);
        if (result < 0) {
            __atomic_store_n(&state->pipeline_task_state[task->slot_idx],
                             TASK_STATE_ERROR,
                             __ATOMIC_RELEASE);
        }

        __atomic_store_n(&state->dispatch_tasks[slot], NULL, __ATOMIC_RELEASE);
        __atomic_store_n(&state->dispatch_cons, cons + 1U, __ATOMIC_RELEASE);
        dispatched++;
    }

    // __atomic_store_n(&state->dispatch_cons, cons + dispatched, __ATOMIC_RELEASE);
#if DMESH_GRPC_PIPELINE_PROFILE
    grpc_profile_report_dispatcher(thread_arg, dispatched);
#endif

    return dispatched;
}

static inline void
publish_consumer_seq(struct dpa_thread_arg *thread_arg, uint64_t consumer_seq)
{
    struct dma_ring_consumer_state *consumer_state;

    consumer_state = (struct dma_ring_consumer_state *)get_dev_ptr(thread_arg->dpa_consumer_state_buf_arr, 0);
    consumer_state->consumer_seq = consumer_seq;
    __dpa_thread_window_writeback();
}

static inline void
init_grpc_task(struct dpa_grpc_serialize_task *task,
                       const struct grpc_req_desc *desc,
                       uint32_t slot_idx)
{
    task->slot_idx = slot_idx;
    task->request_id = desc->req_id;
    task->ring_seq = desc->seq;
    task->src_addr = 0;
    task->len = 0;
    task->schema_id = (uint16_t)desc->schema_id;
    task->flags = DMESH_GRPC_SERIALIZE_TASK_F_COPY_FROM_HOST;
}

static int
prepare_grpc_hello_request_flat(struct dpa_thread_arg *thread_arg,
                           const struct grpc_req_desc *desc,
                           struct dpa_grpc_serialize_task *task)
{
    uint32_t flat_len = desc->size;
    int result;

    if (task == 0)
        return -1;
    if ((desc->addr & (DMESH_GRPC_ARENA_ADDR_ALIGN - 1U)) != 0)
        return -1;
    if (desc->size < sizeof(struct dmesh_grpc_hello_flat) ||
            desc->size > DMESH_GRPC_MAX_FLAT_SIZE)
        return -1;

    task->len = flat_len;
    task->src_addr = desc->addr;

    result = enqueue_grpc_dispatch_task_fields(thread_arg, task);
    if (result < 0)
        return result;

    return 1;
}

static void 
poll_desc_ring_grpc(struct dpa_thread_arg *thread_arg)
{
	struct grpc_req_desc *desc;
	struct dma_ring_consumer_state *consumer_state;
	struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
	uint64_t consumer_seq = 0;
	uint64_t next_desc_seq = 1;
	uint32_t ring_size = thread_arg->buf_arr_size;
#if DEBUG_LOG
	enum pipeline_task_state stall_task_state = TASK_STATE_IDLE;
	uint32_t stall_poll_count = 0;
	uint64_t stall_consumer_seq = 0;
#endif
#if DMESH_GRPC_PIPELINE_PROFILE
	enum pipeline_task_state profile_stall_task_state = TASK_STATE_IDLE;
	uint64_t profile_stall_consumer_seq = (uint64_t)-1;
#endif

	if (state == 0)
	    return;

    consumer_state = (struct dma_ring_consumer_state *)get_dev_ptr(thread_arg->dpa_consumer_state_buf_arr, 0);
	DOCA_DPA_DEV_LOG_INFO("consumer state magic: 0x%lx\n", consumer_state->consumer_seq);
	publish_consumer_seq(thread_arg, consumer_seq);

	if (ring_size > DMESH_GRPC_MAX_PENDING) {
	    DOCA_DPA_DEV_LOG_INFO("ring_size %u exceeds gRPC pending table %u\n",
	                          ring_size, DMESH_GRPC_MAX_PENDING);
	    return;
	}

	/* polling descriptor ring in host memory */
    uint16_t publish_consumer = 0;
	while (1) {
        uint32_t prepared = 0;
        struct dpa_grpc_serialize_task *task;
        enum pipeline_task_state task_state;

#if DEBUG_LOG
        uint8_t retire_blocked = 0;
        uint32_t retire_blocked_idx = 0;
#endif

	    while (next_desc_seq - 1 > consumer_seq) {
	        uint32_t done_idx = (uint32_t)(consumer_seq % DMESH_GRPC_MAX_PENDING);
	            
            task_state = __atomic_load_n(&state->pipeline_task_state[done_idx], __ATOMIC_ACQUIRE);
            if (task_state != TASK_STATE_COMPLETED) {
#if DEBUG_LOG
                retire_blocked = 1;
                retire_blocked_idx = done_idx;
#endif
#if DMESH_GRPC_PIPELINE_PROFILE
                uint8_t new_stall_event =
                    profile_stall_consumer_seq != consumer_seq ||
                    profile_stall_task_state != task_state;

                if (new_stall_event) {
                    profile_stall_consumer_seq = consumer_seq;
                    profile_stall_task_state = task_state;
                }
                grpc_profile_note_retire_stall(state, new_stall_event);
#endif
                break;
            }

            __atomic_store_n(&state->pipeline_task_state[done_idx], TASK_STATE_IDLE, __ATOMIC_RELEASE);

            consumer_seq++;
	        publish_consumer++;
	    }

#if DEBUG_LOG
        if (retire_blocked) {
            uint8_t log_stall = 0;
            uint32_t desc_idx = (uint32_t)(consumer_seq % ring_size);
            struct grpc_req_desc *blocked_desc =
                (struct grpc_req_desc *)get_dev_ptr(thread_arg->dpa_buf_arr, desc_idx);

            if (stall_consumer_seq != consumer_seq ||
                    stall_task_state != task_state) {
                stall_consumer_seq = consumer_seq;
                stall_task_state = task_state;
                stall_poll_count = 0;
            } else if (stall_poll_count != UINT32_MAX) {
                stall_poll_count++;
            }

            if (task_state == TASK_STATE_ERROR ||
                    stall_poll_count == DMESH_GRPC_STALL_LOG_POLL_INTERVAL ||
                    (stall_poll_count > DMESH_GRPC_STALL_LOG_POLL_INTERVAL &&
                     (stall_poll_count % DMESH_GRPC_STALL_LOG_POLL_INTERVAL) == 0U)) {
                log_stall = 1;
            }

            if (log_stall) {
                uint64_t desc_seq = 0;
                uint64_t desc_addr = 0;
                uint64_t desc_size = 0;
                uint32_t desc_req = 0;
                uint16_t desc_schema = 0;

                if (blocked_desc != 0) {
                    desc_seq = blocked_desc->seq;
                    desc_addr = blocked_desc->addr;
                    desc_size = blocked_desc->size;
                    desc_req = blocked_desc->req_id;
                    desc_schema = blocked_desc->schema_id;
                }

                DOCA_DPA_DEV_LOG_INFO("gRPC retire stalled: consumer_seq=%lu expected_seq=%lu "
                                      "next_desc_seq=%lu pipeline_task_state[%u]=%u "
                                      "desc_idx=%u ring_seq=%lu req_id=%u schema=%u "
                                      "addr=0x%lx size=%lu polls=%u\n",
                                      consumer_seq, consumer_seq + 1U,
                                      next_desc_seq, retire_blocked_idx,
                                      (uint32_t)task_state, desc_idx,
                                      desc_seq, desc_req, desc_schema,
                                      desc_addr, desc_size, stall_poll_count);
            }
        } else {
            stall_poll_count = 0;
        }
#endif

        if (publish_consumer > DMA_COMPLETION_BATCH_SIZE) {
            struct dpa_grpc_serialize_task *done_task = &grpc_tasks[(consumer_seq - 1) % DMESH_GRPC_MAX_PENDING];
            struct comch_grpc_serialize_comp_msg ser_comp_msg = 
                (struct comch_grpc_serialize_comp_msg){
                    .type = COMCH_MSG_TYPE_GRPC_SERIALIZE_COMPLETED,
                    .request_id = done_task->request_id,
                    .ring_seq = done_task->ring_seq,
                    .encoded_len = done_task->len,
                    .schema_id = done_task->schema_id,
                    .completed = publish_consumer, // use status field to piggyback the number of completed descriptors
                };

            doca_dpa_dev_comch_producer_post_send_imm_only(thread_arg->dpa_producer,
                                                   /*consumer_id=*/1,
                                                   (uint8_t *)&ser_comp_msg,
                                                   sizeof(ser_comp_msg),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
                                                   
            
            // doca_dpa_dev_completion_element_t comp;
            // while(doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) == 0);
            // doca_dpa_dev_completion_ack(thread_arg->dpa_producer_comp, 1);

            publish_consumer_seq(thread_arg, consumer_seq);
            publish_consumer = 0;
        }

        while (next_desc_seq - consumer_seq <= ring_size &&
               prepared < DMA_COMPLETION_BATCH_SIZE) {
            uint32_t desc_idx = (uint32_t)((next_desc_seq - 1U) % ring_size);
            int prep_result;

            if (__atomic_load_n(&state->pipeline_task_state[desc_idx], __ATOMIC_ACQUIRE) 
                != TASK_STATE_IDLE)
                break;

            desc = (struct grpc_req_desc *)get_dev_ptr(thread_arg->dpa_buf_arr, desc_idx);
            if (desc->seq != next_desc_seq) {
                /*
                * Host descriptor publish is pure shared-memory polling; no
                * completion wakes this path when the ring is empty.
                */
               break;
            }

            if (desc->schema_id > grpc_schema_blob.msg_count) {
                DOCA_DPA_DEV_LOG_INFO("gRPC serialize unsupported schema: req=%u seq=%lu schema=%u\n",
                                    desc->req_id, desc->seq, desc->schema_id);
                return;
            }

#if DEBUG_LOG            
            if (next_desc_seq <= 8U || (next_desc_seq % DEBUG_INTERVAL) == 0U) {
                DOCA_DPA_DEV_LOG_INFO("gRPC desc seen: seq=%lu idx=%u schema=%u req=%u addr=0x%lx size=%u\n",
                                      desc->seq, desc_idx, desc->schema_id,
                                      desc->req_id, desc->addr, desc->size);
            }
#endif

            /*
            * Descriptor ownership remains with DPA until serialization finishes.
            * Host arena memory may be DMA source for pointer fields, so
            * consumer_seq advances only when this sequence is completed.
            */
            if (desc->schema_id <= grpc_schema_blob.msg_count) {
                __atomic_store_n(&state->pipeline_task_state[desc_idx], TASK_STATE_PROCESSING, __ATOMIC_RELEASE);
                task = &grpc_tasks[desc_idx];
                init_grpc_task(task, desc, desc_idx);

                prep_result = prepare_grpc_hello_request_flat(thread_arg, desc, task);
#if DEBUG_LOG                                                            
                if (next_desc_seq <= 8U || (next_desc_seq % DEBUG_INTERVAL) == 0U) {
                    DOCA_DPA_DEV_LOG_INFO("gRPC prepare result: next_desc_seq=%lu consumer_seq=%lu ring_seq=%lu "
                                            "req=%u schema=%u\n",
                                            next_desc_seq, consumer_seq,             
                                            task->ring_seq, 
                                            task->request_id,
                                            task->schema_id);
                }
#endif
            
            }

            else
                prep_result = -1;

            if (prep_result <= 0) {
                DOCA_DPA_DEV_LOG_INFO("Failed to prepare gRPC descriptor: seq=%lu\n", next_desc_seq);
                __atomic_store_n(&state->pipeline_task_state[desc_idx], TASK_STATE_COMPLETED, __ATOMIC_RELEASE);
            }

            next_desc_seq++;
            prepared++;
        }

        if (prepared == 0) {
            __dpa_thread_window_read_inv();
        }

        while (drain_dpa_producer_completions(thread_arg) > 0);
	}
}

static void 
complete_grpc_serialize_task(struct dpa_thread_arg *thread_arg,
                            struct dpa_grpc_serialize_task *task,
                            uint8_t mode)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    ProtoTask proto_task = {0};
    ProtoCompletion proto_cpl = {0};
    struct grpc_dpa_copy_ctx copy_ctx;
    int result = 0;
#if DEBUG_LOG
    uint64_t out_offset = 0;
#endif
    uint32_t copy_len;
    uint32_t done_idx;
    uint32_t slot_idx;

    __dpa_thread_window_read_inv();

#if DEBUG_LOG    
    if (task->ring_seq <= 8U || (task->ring_seq % DEBUG_INTERVAL) == 0U) {
        DOCA_DPA_DEV_LOG_INFO("Completing gRPC serialize task: req=%u seq=%lu\n",
                            task->request_id, task->ring_seq);
    }
#endif

    slot_idx = task->slot_idx;
    if (slot_idx >= DMESH_GRPC_MAX_PENDING) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize invalid slot: req=%u seq=%lu slot=%u\n",
                              task->request_id, task->ring_seq, slot_idx);
        return;
    }
    done_idx = slot_idx;

    uint64_t flat_addr = thread_arg->dpu_base_addr + (slot_idx * DMESH_GRPC_PRIVATE_SLOT_SIZE);
    uint64_t out_addr = flat_addr + DMA_ALIGN_UP(DMESH_GRPC_MAX_FLAT_SIZE);
    copy_len = DMA_ALIGN_UP(task->len);

    uint64_t flat = (uint64_t)dpa_dpu_ptr(thread_arg, flat_addr, task->len);
    uint64_t out = (uint64_t)dpa_dpu_ptr(thread_arg, out_addr, DMESH_GRPC_MAX_ENCODED_SIZE);
    if (flat == 0 || out == 0) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize invalid addresses: req=%u seq=%lu flat=0x%lx out=0x%lx\n",
                                task->request_id, task->ring_seq,
                                flat_addr, out_addr);
        result = -1;
        proto_cpl.status = result;
        proto_cpl.encoded_len = 0U;
        goto finish;
    }

    proto_task.request_id = task->request_id;
    proto_task.schema_id = task->schema_id;
    proto_task.flat = flat;
    proto_task.out = out;
    copy_ctx.thread_arg = thread_arg;
    copy_ctx.done_idx = done_idx;

    if (task->flags & DMESH_GRPC_SERIALIZE_TASK_F_COPY_FROM_HOST) {
        if (mode <= DMESH_GRPC_SERIALIZE_MODE_GRPC_REVERSE) {
            result = submit_grpc_host_to_dpu_copy(thread_arg,
                                                flat_addr,
                                                task->src_addr,
                                                copy_len);
            if (result < 0) {
                proto_cpl.status = result;
                proto_cpl.encoded_len = 0U;
                goto finish;
            }
            __dpa_thread_window_read_inv();

        } else {
            proto_task.flat = task->src_addr;
            proto_task.out = out_addr;
            copy_ctx.from_host = 1;
        }

    } else if (mode > DMESH_GRPC_SERIALIZE_MODE_GRPC_REVERSE) {
        proto_task.flat = flat_addr;
        proto_task.out = out_addr;
        copy_ctx.from_host = 0;
    }

    switch (mode) {
    case DMESH_GRPC_SERIALIZE_MODE_GRPC:
        result = grpc_wire_serialize_one(&grpc_schema_blob, &proto_task, &proto_cpl, NULL);
        break;
    case DMESH_GRPC_SERIALIZE_MODE_GRPC_REVERSE:
        result = grpc_wire_serialize_one_reverse(&grpc_schema_blob, &proto_task, &proto_cpl, NULL);
#if DEBUG_LOG
        if (result > 0) {
            out_offset = result;
        }
#endif
        break;
    case DMESH_GRPC_SERIALIZE_MODE_COPY:
        result = grpc_wire_serialize_one_copy(&proto_task, task->len,
                                              &proto_cpl, submit_grpc_post_copy,
                                              &copy_ctx);
        break;
    case DMESH_GRPC_SERIALIZE_MODE_DMA_COPY:
        result = grpc_wire_serialize_one_copy(&proto_task, task->len,
                                              &proto_cpl, submit_grpc_dma_copy,
                                              &copy_ctx);
        break;
    default:
        proto_cpl.status = -4;
        proto_cpl.encoded_len = 0U;
        result = proto_cpl.status;
        break;
    }

finish:
    __dpa_thread_window_writeback();

    task->len = proto_cpl.encoded_len; // flat_len -> encoded_len
    __sync_synchronize();

    if (result < 0) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize failed: req=%u seq=%lu schema=%u error=%d\n",
                                task->request_id, task->ring_seq, task->schema_id, result);
                                
        __atomic_store_n(&state->pipeline_task_state[done_idx], TASK_STATE_ERROR, __ATOMIC_RELEASE);
        return;
    }
    __atomic_store_n(&state->pipeline_task_state[done_idx], TASK_STATE_COMPLETED, __ATOMIC_RELEASE);


#if DEBUG_LOG
    if (task->ring_seq <= 8U || (task->ring_seq % DEBUG_INTERVAL) == 0U) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize done: "
                                "req=%u seq=%lu slot=%u status=%d len=%u out_offset=%lu\n",
                                task->request_id, task->ring_seq, done_idx,
                                proto_cpl.status, proto_cpl.encoded_len, out_offset);
    }
#endif
}

static void 
poll_grpc_serializer_queue(struct dpa_thread_arg *thread_arg)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    uint32_t worker = thread_arg->serializer_index;
    uint32_t idle_polls = 0;

    for (;;) {
        uint32_t cons = __atomic_load_n(&state->serializer_cons[worker], __ATOMIC_ACQUIRE);
        uint32_t prod = __atomic_load_n(&state->serializer_prod[worker], __ATOMIC_ACQUIRE);
        uint32_t slot;
        struct dpa_grpc_serialize_task *task;

        if (cons == prod) {
            if (++idle_polls >= DMESH_GRPC_SERIALIZER_IDLE_POLL_BUDGET) {
#if DMESH_GRPC_PIPELINE_PROFILE
                grpc_profile_note_serializer_idle_poll(state, worker);
                grpc_profile_note_serializer_reschedule(state, worker);
#endif
                /*
                 * Serializer workers are event-driven by notification
                 * completions. After a bounded empty-queue poll window, yield
                 * the EU and rely on the msg worker's next enqueue notify to
                 * activate this thread again. A notify racing with this
                 * reschedule is assumed to remain attached to the notification
                 * completion and retrigger the thread.
                 */
                doca_dpa_dev_thread_reschedule();
                return;
            }
#if DMESH_GRPC_PIPELINE_PROFILE
            grpc_profile_note_serializer_idle_poll(state, worker);
#endif
            __dpa_thread_window_read_inv();
            continue;
        }
        idle_polls = 0;
        
        slot = cons % DMESH_GRPC_SERIALIZER_QUEUE_DEPTH;
        if (__atomic_load_n(&state->serializer_tasks[worker][slot], __ATOMIC_ACQUIRE) == NULL) {
            __dpa_thread_window_read_inv();
            continue;
        }
        task = state->serializer_tasks[worker][slot];
        complete_grpc_serialize_task(thread_arg, task, DMESH_GRPC_SERIALIZE_MODE_COPY);
#if DMESH_GRPC_PIPELINE_PROFILE
        grpc_profile_note_serializer_completed(state, worker);
#endif

        __atomic_store_n(&state->serializer_tasks[worker][slot], NULL, __ATOMIC_RELEASE);
        __atomic_store_n(&state->serializer_cons[worker], cons + 1U, __ATOMIC_RELEASE);
    }
}

static void 
poll_desc_ring(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    struct comch_dma_comp_msg msg;
    doca_dpa_dev_uintptr_t dev_ptr;
    doca_dpa_dev_buf_t buf;
    struct dma_desc *desc;
    struct dma_ring_consumer_state *consumer_state;
    uint64_t consumer_seq = 0;
    uint32_t pending_completions = 0;
    uint32_t ring_size = thread_arg->buf_arr_size;
    uint32_t buf_size = thread_arg->buf_size;

    buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_consumer_state_buf_arr, 0);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    consumer_state = (struct dma_ring_consumer_state *)dev_ptr;
    DOCA_DPA_DEV_LOG_INFO("consumer state magic: 0x%lx\n", consumer_state->consumer_seq);
    publish_consumer_seq(thread_arg, consumer_seq);

    /* polling descriptor ring in host memory */
    while (1) {
        uint32_t desc_idx = consumer_seq % ring_size;
        uint64_t expected_seq = consumer_seq + 1;
        uint64_t desc_seq;
        uint64_t desc_addr;
        uint64_t desc_size;

        buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, desc_idx);
        dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
        desc = (struct dma_desc *)dev_ptr;

        while ((desc_seq = desc->seq) != expected_seq) {
            __dpa_thread_window_read_inv();

            if (pending_completions > 0) {
                publish_consumer_seq(thread_arg, consumer_seq);
                pending_completions = 0;

                buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, desc_idx);
                dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
                desc = (struct dma_desc *)dev_ptr;
            }
        }
        desc_addr = desc->addr;
        desc_size = desc->size;

        /* if consumer is empty, wait */
        wait_for_dpu_consumer(thread_arg);

        if (thread_arg->pos + desc_size > buf_size) {
            // DOCA_DPA_DEV_LOG_INFO("Reached end of buffer, resetting position\n");
            thread_arg->pos = 0;
        }

        msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
        msg.pos = thread_arg->pos;
        msg.length = (uint32_t)desc_size;

        doca_dpa_dev_comch_producer_dma_copy(producer,
                                    /*consumer_id=*/1,
                                    thread_arg->dpu_mmap,
                                    thread_arg->src_addr + thread_arg->pos,
                                    thread_arg->host_mmap,
                                    desc_addr,
                                    desc_size,
                                    (uint8_t *)&msg,
                                    sizeof(struct comch_dma_comp_msg),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | 
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

        /*
         * Descriptor ownership is host-write/DPA-read. Advancing consumer_seq
         * after submit assumes the DOCA producer copied request arguments into
         * its work item; if it only references desc memory, this must move to a
         * producer completion path.
         */
        thread_arg->pos += desc_size;
        consumer_seq++;
        pending_completions++;
        if (pending_completions >= DMA_COMPLETION_BATCH_SIZE ||
            consumer_seq % ring_size == 0) {
            // DOCA_DPA_DEV_LOG_INFO("Acknowledging %u completed descriptors, consumer_seq: %lu\n",
            //                     pending_completions, consumer_seq);
            publish_consumer_seq(thread_arg, consumer_seq);
            pending_completions = 0;
        }
    }
}

/*---------------------------------------------------------------------------*/
__dpa_global__ void hello_world(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("DPA buffer array handle: 0x%lx\n", thread_arg->dpa_buf_arr);
    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("Starting DMA manager thread...\n");
    DOCA_DPA_DEV_LOG_INFO("DPA buffer array handle: 0x%lx, size: %u\n", thread_arg->dpa_buf_arr, thread_arg->buf_arr_size);
    DOCA_DPA_DEV_LOG_INFO("DPU mmap: %u, addr: %lu host mmap: %u\n", thread_arg->dpu_mmap, thread_arg->src_addr, thread_arg->host_mmap);

    if (POLL_GRPC) {
        poll_desc_ring_grpc(thread_arg);
    } else {
        poll_desc_ring(thread_arg);
    }

    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_grpc_desc_main(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("Starting gRPC descriptor main thread: idx=%u\n",
                          thread_arg->thread_index);
    poll_desc_ring_grpc(thread_arg);
    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_grpc_msg_worker(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    // DOCA_DPA_DEV_LOG_INFO("Starting gRPC msg worker thread: idx=%u\n",
    //                       thread_arg->thread_index);
    for (;;) {
        uint32_t dispatched;

        handle_msgs(thread_arg);
        dispatched = dispatch_grpc_serialize_tasks(thread_arg, 0);
        if (dispatched == 0U) {
            /*
             * Once woken by main, the dispatcher stays resident even though it
             * also owns Comch completions. Rescheduling here would make
             * shared-memory dispatch progress depend on a later completion
             * event instead of the dispatch queue itself.
             */
            __dpa_thread_window_read_inv();
        }
    }
}

__dpa_global__ void run_grpc_serializer_worker(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    // DOCA_DPA_DEV_LOG_INFO("Starting gRPC serializer worker thread: idx=%u serializer=%u\n",
    //                       thread_arg->thread_index, thread_arg->serializer_index);
    poll_grpc_serializer_queue(thread_arg);
}
