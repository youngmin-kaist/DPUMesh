#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"
#include "grpc_wire_encode.h"

#include <stddef.h>

/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @return: returns RPC_RETURN_STATUS_SUCCESS on success and RPC_RETURN_STATUS_ERROR otherwise
 */

static inline uint32_t bswap32(uint32_t x) {
    return ((x & 0x000000FF) << 24) |
           ((x & 0x0000FF00) << 8)  |
           ((x & 0x00FF0000) >> 8)  |
           
           ((x & 0xFF000000) >> 24);
}

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

static void send_msgs(struct dpa_thread_arg *thread_arg, int num_msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    doca_dpa_dev_completion_element_t comp;
    struct comch_msg msg;
    uint64_t start, end, tick;
    doca_dpa_dev_completion_type_t t;
    tick = __dpa_thread_time();

    DOCA_DPA_DEV_LOG_INFO("tick frequency: %lu\n", tick);
    for (int i = 0; i < num_msg; i++) {
        start =  __dpa_thread_time();
        doca_dpa_dev_comch_producer_post_send_imm_only(producer,
                                                /*consumer_id=*/1,
                                                (uint8_t *)&msg,
                                                sizeof(struct comch_msg),
                                                DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            								    //    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);
        
        while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) == 0) {
        }

        end =  __dpa_thread_time();
        t = doca_dpa_dev_get_completion_type(comp);
                                                
        DOCA_DPA_DEV_LOG_INFO("type: %d, cycles: %lu\n", t, end - start);
    }
}

/*
 * The current runtime starts one DMA-manager DPA thread. DOCA DMA send may keep
 * the immediate-data pointer until completion, so outstanding requests cannot
 * use stack metadata. Make this table per-thread before running multiple DMA
 * managers in one DPA process.
 */
static struct comch_grpc_dma_comp_msg grpc_dma_comp_imm[DMESH_GRPC_MAX_PENDING];

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

static inline doca_dpa_dev_uintptr_t 
dpa_host_ptr(struct dpa_thread_arg *thread_arg, uint64_t addr, uint64_t size)
{
    return dpa_mmap_ptr(thread_arg->dpa_host_mmap_buf_arr,
                        thread_arg->host_base_addr,
                        thread_arg->host_buf_size,
                        addr,
                        size);
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

static inline void 
post_grpc_field_dma(struct dpa_thread_arg *thread_arg,
                                uint64_t dst_addr,
                                uint64_t src_addr,
                                uint64_t len,
                                const struct comch_grpc_dma_comp_msg *msg,
                                enum doca_dpa_dev_submit_flag flags)
{
    doca_dpa_dev_comch_producer_dma_copy(thread_arg->dpa_producer,
                                         /*consumer_id=*/1,
                                         thread_arg->dpu_mmap,
                                         dst_addr,
                                         thread_arg->host_mmap,
                                         src_addr,
                                         len,
                                         (uint8_t *)msg,
                                         sizeof(*msg),
                                         flags);
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
enqueue_grpc_serialize_task(struct dpa_thread_arg *thread_arg,
                                       const struct comch_grpc_serialize_req_msg *req)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    uint32_t worker = state->serializer_drr_cursor % DMESH_GRPC_SERIALIZER_THREADS;
    uint32_t cost = grpc_serialize_task_cost(req->len);
    uint32_t deficit = state->serializer_drr_deficit[worker];
    uint32_t prod;
    uint32_t cons;

    // find a worker with enough deficit to run the task, using a DRR-like approach
    while (deficit < cost) {
        deficit = drr_add_quantum(deficit);
        state->serializer_drr_deficit[worker] = deficit;
        
        worker = (worker + 1U) % DMESH_GRPC_SERIALIZER_THREADS;
        state->serializer_drr_cursor = worker;
    }
        
    // wait for an empty slot in the selected worker's queue
    for (;;) {
        prod = __atomic_load_n(&state->serializer_prod[worker], __ATOMIC_ACQUIRE);
        cons = __atomic_load_n(&state->serializer_cons[worker], __ATOMIC_ACQUIRE);

        if (prod - cons < DMESH_GRPC_SERIALIZER_QUEUE_DEPTH)
            break;
    }

    // publish the task to the selected worker's queue
    uint32_t slot = prod % DMESH_GRPC_SERIALIZER_QUEUE_DEPTH;
    struct dpa_grpc_serialize_task *task = &state->serializer_tasks[worker][slot];
    if (__atomic_load_n(&task->valid, __ATOMIC_ACQUIRE) != 0U) {
        DOCA_DPA_DEV_LOG_INFO("[%u] Unexpected valid task in serializer queue: slot=%u prod=%u cons=%u\n",
                                worker, slot, prod, cons);
        return -1;
    }

    uint64_t idx = (req->ring_seq - 1U) % DMESH_GRPC_MAX_PENDING;
    enum pipeline_task_state cur_state = __atomic_load_n(&state->pipeline_task_state[idx], __ATOMIC_ACQUIRE);
    if (cur_state != TASK_STATE_PROCESSING) {
        DOCA_DPA_DEV_LOG_INFO("[%u] Invalid pipeline task state for gRPC serialize request: req=%u seq=%lu state=%u\n",
                                worker, req->request_id, req->ring_seq, cur_state);
        __atomic_store_n(&state->pipeline_task_state[idx], TASK_STATE_ERROR, __ATOMIC_RELEASE);
        return -1;
    }
    
    task->request_id = req->request_id;
    task->ring_seq = req->ring_seq;
    task->len = req->len;
    task->schema_id = req->schema_id;
    
#if DEBUG_LOG    
    if (req->ring_seq <= 8U || (req->ring_seq % DEBUG_INTERVAL) == 0U) {
        DOCA_DPA_DEV_LOG_INFO("[%u] Enqueued gRPC serialize task: req=%u seq=%lu len=%u cost=%u deficit=%u\n",
            worker, req->request_id, req->ring_seq, req->len, cost, deficit);
    }
#endif
        
    /*
    * The msg worker is the single producer for all serializer queues.
    * valid/prod are release-published; each serializer worker consumes
    * only its own queue with acquire loads.
    */
    __atomic_store_n(&task->valid, 1U, __ATOMIC_RELEASE);
    __atomic_store_n(&state->serializer_prod[worker], prod + 1U, __ATOMIC_RELEASE);
    __sync_synchronize();
    state->serializer_drr_deficit[worker] = deficit - cost;
    
    // notify the serializer worker thread
    if (thread_arg->serializer_notify[worker] != 0)
        doca_dpa_dev_thread_notify(thread_arg->serializer_notify[worker]);
    
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

static void 
handle_msgs(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_consumer_completion_element_t completion;
    struct comch_msg *msg;
    uint32_t msg_size;
    doca_dpa_dev_comch_consumer_t consumer = thread_arg->dpa_consumer;
	doca_dpa_dev_comch_consumer_completion_t consumer_comp = thread_arg->dpa_consumer_comp;
    uint32_t num_msgs = 0;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = (struct comch_msg *)doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    // send_msgs(thread_arg, num_msgs);
    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }
    // DOCA_DPA_DEV_LOG_INFO("Handled %u msgs from host\n", num_msgs);
}

static int 
handle_msgs_ser(struct dpa_thread_arg *thread_arg)
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

static inline void
publish_consumer_seq(struct dpa_thread_arg *thread_arg, uint64_t consumer_seq)
{
    struct dma_ring_consumer_state *consumer_state;

    consumer_state = (struct dma_ring_consumer_state *)get_dev_ptr(thread_arg->dpa_consumer_state_buf_arr, 0);
    consumer_state->consumer_seq = consumer_seq;
    __dpa_thread_window_writeback();
}

static inline void
init_grpc_dma_comp_msg(struct comch_grpc_dma_comp_msg *comp_msg,
                       const struct grpc_req_desc *desc,
                       uint64_t ring_seq)
{
    comp_msg->type = COMCH_MSG_TYPE_GRPC_DMA_COMPLETED;
    comp_msg->request_id = desc->req_id;
    comp_msg->ring_seq = ring_seq;
    comp_msg->len = 0;
    comp_msg->schema_id = (uint16_t)desc->schema_id;
    comp_msg->expected_dma = 0;
}

static int
prepare_grpc_hello_request_flat(struct dpa_thread_arg *thread_arg,
                           const struct grpc_req_desc *desc,
                           uint32_t slot_idx,
                           struct comch_grpc_dma_comp_msg *comp_msg)
{
    uint32_t flat_len = desc->size;
    uint32_t copy_len = DMA_ALIGN_UP(flat_len);
    uint64_t flat_addr = thread_arg->dpu_base_addr + ((uint64_t)slot_idx * DMESH_GRPC_PRIVATE_SLOT_SIZE);

    if (comp_msg == 0)
        return -1;
    if ((desc->addr & (DMESH_GRPC_ARENA_ADDR_ALIGN - 1U)) != 0)
        return -1;
    if (((uint64_t)slot_idx + 1U) * DMESH_GRPC_PRIVATE_SLOT_SIZE > thread_arg->buf_size)
        return -1;
    if (desc->size < sizeof(struct dmesh_grpc_hello_flat) ||
            desc->size > DMESH_GRPC_MAX_FLAT_SIZE)
        return -1;

    comp_msg->len = flat_len;
    comp_msg->expected_dma = 1;

    wait_for_dpu_consumer(thread_arg);
    post_grpc_field_dma(thread_arg,
                        flat_addr,
                        desc->addr,
                        copy_len,
                        comp_msg,
                        DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS |
                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    return 1;
}


static int
prepare_grpc_hello_request(struct dpa_thread_arg *thread_arg,
                           const struct grpc_req_desc *desc,
                           uint32_t slot_idx,
                           struct comch_grpc_dma_comp_msg *comp_msg)
{
    const struct dmesh_grpc_hello_request *req;
    struct dmesh_grpc_hello_flat *flat;
    uint64_t id;
    uint64_t name_addr;
    uint64_t name_dma_len = 0;
    uint64_t scores_addr;
    uint64_t scores_dma_len = 0;

    uint32_t expected_dma = 0;
    uint32_t name_len;
    uint32_t scores_count;
    uint32_t scores_bytes;
    uint32_t name_off = 0;
    uint32_t scores_off = 0;
    enum doca_dpa_dev_submit_flag dma_flags;

    uint64_t flat_len = DMA_ALIGN_UP(sizeof(struct dmesh_grpc_hello_flat));
    uint64_t flat_addr = thread_arg->dpu_base_addr + ((uint64_t)slot_idx * DMESH_GRPC_PRIVATE_SLOT_SIZE);
    uint64_t flat_base = flat_addr + flat_len; 
    uint64_t out_addr = flat_addr + DMA_ALIGN_UP(DMESH_GRPC_MAX_FLAT_SIZE);

    if (comp_msg == 0)
        return -1;
    if ((desc->addr & (DMESH_GRPC_ARENA_ADDR_ALIGN - 1U)) != 0)
        return -1;
    if (((uint64_t)slot_idx + 1U) * DMESH_GRPC_PRIVATE_SLOT_SIZE > thread_arg->buf_size)
        return -1;

    req = (const struct dmesh_grpc_hello_request *)dpa_host_ptr(thread_arg,
                                                               desc->addr,
                                                               sizeof(*req));
    if (req == 0)
        return -1;

    id = req->id;
    name_addr = (uint64_t)req->name;
    name_len = req->name_len;
    scores_addr = (uint64_t)req->scores;
    scores_count = req->scores_count;
    scores_bytes = scores_count << 2; // u32

    if (name_len != 0U) {
        name_off = (uint32_t)flat_len;
        name_dma_len = DMA_ALIGN_UP((uint64_t)name_len);
        flat_len += name_dma_len;
        expected_dma++;
    }
    if (scores_bytes != 0U) {
        scores_off = (uint32_t)flat_len;
        scores_dma_len = DMA_ALIGN_UP((uint64_t)scores_bytes);
        flat_len += scores_dma_len;
        expected_dma++;
    }
    if (flat_len > DMESH_GRPC_MAX_FLAT_SIZE)
        return -1;

    comp_msg->expected_dma = (uint16_t)expected_dma;
    comp_msg->len = (uint32_t)flat_len;

    flat = (struct dmesh_grpc_hello_flat *)dpa_dpu_ptr(thread_arg, flat_addr, flat_len);
    if (flat == 0)
        return -1;

    flat->id = id;
    flat->name.offset = name_off;
    flat->name.len = name_len;
    flat->scores.offset = scores_off;
    flat->scores.count = scores_count;
    __dpa_thread_window_writeback();

    wait_for_dpu_consumer(thread_arg);
    
    if (expected_dma == 0U) {
        doca_dpa_dev_comch_producer_post_send_imm_only(thread_arg->dpa_producer,
                                                    /*consumer_id=*/1,
                                                    (uint8_t *)comp_msg,
                                                    sizeof(*comp_msg),
                                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        return 1;
    }

    dma_flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS;
    if (name_len != 0U) {
        if (expected_dma == 1U) {
            dma_flags |= DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH;
        }
        post_grpc_field_dma(thread_arg,
                            flat_addr + name_off,
                            name_addr,
                            name_dma_len,
                            comp_msg,
                            dma_flags); 
    }
    if (scores_bytes != 0U) {
        dma_flags |= DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH;
        post_grpc_field_dma(thread_arg,
                            flat_addr + scores_off,
                            scores_addr,
                            scores_dma_len,
                            comp_msg,
                            dma_flags);
    }

    // comp_msg->expected_dma = 1;
    // doca_dpa_dev_comch_producer_dma_copy(thread_arg->dpa_producer,
    //                             /*consumer_id=*/1,
    //                             thread_arg->dpu_mmap,
    //                             flat_base,
    //                             thread_arg->host_mmap,
    //                             name_addr,
    //                             name_dma_len + scores_dma_len,
    //                             (uint8_t *)comp_msg,
    //                             sizeof(*comp_msg),
    //                             DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

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
	enum pipeline_task_state stall_task_state = TASK_STATE_IDLE;
	uint32_t ring_size = thread_arg->buf_arr_size;
#if DEBUG_LOG
	uint32_t stall_poll_count = 0;
	uint64_t stall_consumer_seq = 0;
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
        struct comch_grpc_dma_comp_msg *dma_comp_msg;
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

        if (publish_consumer > 64) {
            struct comch_grpc_serialize_comp_msg *msg = 
                (struct comch_grpc_serialize_comp_msg *)&grpc_dma_comp_imm[(consumer_seq - 1) % DMESH_GRPC_MAX_PENDING];
            struct comch_grpc_serialize_comp_msg ser_comp_msg = 
                (struct comch_grpc_serialize_comp_msg){
                    .type = COMCH_MSG_TYPE_GRPC_SERIALIZE_COMPLETED,
                    .request_id = msg->request_id,
                    .ring_seq = msg->ring_seq,
                    .encoded_len = msg->encoded_len,
                    .schema_id = msg->schema_id,
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
// TODO: valid schema_id check
            if (desc->schema_id > grpc_schema_blob.msg_count) {
                DOCA_DPA_DEV_LOG_INFO("gRPC serialize unsupported schema: req=%u seq=%lu schema=%u\n",
                                    desc->req_id, desc->seq, desc->schema_id);
                return;
            }

#if DEBUG_LOG            
            if (next_desc_seq <= 8U || (next_desc_seq % DEBUG_INTERVAL) == 0U) {
                DOCA_DPA_DEV_LOG_INFO("gRPC desc seen: seq=%lu idx=%u schema=%u req=%u addr=0x%lx size=%lu\n",
                                      desc->seq, desc_idx, desc->schema_id,
                                      desc->req_id, desc->addr, desc->size);
            }
#endif
            __atomic_store_n(&state->pipeline_task_state[desc_idx], TASK_STATE_PROCESSING, __ATOMIC_RELEASE);

            /*
             * Descriptor ownership remains with DPA until serialization finishes.
             * Host arena memory may be DMA source for pointer fields, so
             * consumer_seq advances only when this sequence is completed.
             */
            if (desc->schema_id <= grpc_schema_blob.msg_count) {
                dma_comp_msg = &grpc_dma_comp_imm[desc_idx];
                init_grpc_dma_comp_msg(dma_comp_msg, desc, next_desc_seq);

                prep_result = prepare_grpc_hello_request_flat(thread_arg, desc,
                                                            desc_idx, dma_comp_msg);
                // prep_result = prepare_grpc_hello_request(thread_arg, desc,
                //                                             desc_idx, dma_comp_msg);

#if DEBUG_LOG                                                            
                if (next_desc_seq <= 8U || (next_desc_seq % DEBUG_INTERVAL) == 0U) {
                    DOCA_DPA_DEV_LOG_INFO("gRPC prepare result: next_desc_seq=%lu consumer_seq=%lu ring_seq=%lu "
                                            "req=%u schema=%u expected_dma=%u\n",
                                            next_desc_seq, consumer_seq,             
                                            dma_comp_msg->ring_seq, 
                                            dma_comp_msg->request_id,
                                            dma_comp_msg->schema_id,
                                            (uint32_t)dma_comp_msg->expected_dma);
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
                            uint8_t reverse)
{
    struct dpa_grpc_pipeline_state *state = grpc_pipeline_state(thread_arg);
    ProtoTask proto_task = {0};
    ProtoCompletion proto_cpl = {0};
    int result = 0;
    uint64_t out_offset = 0;
    struct comch_grpc_serialize_comp_msg *comp_msg;
    uint32_t done_idx;

    __dpa_thread_window_read_inv();

#if DEBUG_LOG    
    if (task->ring_seq <= 8U || (task->ring_seq % DEBUG_INTERVAL) == 0U) {
        DOCA_DPA_DEV_LOG_INFO("Completing gRPC serialize task: req=%u seq=%lu\n",
                            task->request_id, task->ring_seq);
    }
#endif

    uint64_t slot_idx = (task->ring_seq - 1) % DMESH_GRPC_MAX_PENDING;
    uint64_t flat_addr = thread_arg->dpu_base_addr + (slot_idx * DMESH_GRPC_PRIVATE_SLOT_SIZE);
    uint64_t out_addr = flat_addr + DMA_ALIGN_UP(DMESH_GRPC_MAX_FLAT_SIZE);

    uint64_t flat = (uint64_t)dpa_dpu_ptr(thread_arg, flat_addr, task->len);
    uint64_t out = (uint64_t)dpa_dpu_ptr(thread_arg, out_addr, DMESH_GRPC_MAX_ENCODED_SIZE);
    if (flat == 0 || out == 0) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize invalid addresses: req=%u seq=%lu flat=0x%lx out=0x%lx\n",
                                task->request_id, task->ring_seq,
                                flat_addr, out_addr);
        return;
    }

    proto_task.request_id = task->request_id;
    proto_task.schema_id = task->schema_id;
    proto_task.flat = flat;
    proto_task.out = out;

    if (reverse) {
        result = grpc_wire_serialize_one_reverse(&grpc_schema_blob, &proto_task, &proto_cpl, NULL);
        if (result > 0) {
            out_offset = result;
        }
    } else {
        result = grpc_wire_serialize_one(&grpc_schema_blob, &proto_task, &proto_cpl, NULL);
    }
    __dpa_thread_window_writeback();


    done_idx = (uint32_t)((task->ring_seq - 1U) % DMESH_GRPC_MAX_PENDING);
    comp_msg = (struct comch_grpc_serialize_comp_msg *)&grpc_dma_comp_imm[done_idx];
    comp_msg->completed = 0;
    comp_msg->encoded_len = proto_cpl.encoded_len;
    __sync_synchronize();

    if (result < 0) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize failed: req=%u seq=%lu schema=%u error=%d\n",
                                task->request_id, task->ring_seq, task->schema_id, result);
                                
        __atomic_store_n(&state->pipeline_task_state[done_idx], TASK_STATE_ERROR, __ATOMIC_RELEASE);
        return;
    }
    __atomic_store_n(&state->pipeline_task_state[done_idx], TASK_STATE_COMPLETED, __ATOMIC_RELEASE);


#if DEBUG_LOG
    if (comp_msg->ring_seq <= 8U || (comp_msg->ring_seq % DEBUG_INTERVAL) == 0U) {
        DOCA_DPA_DEV_LOG_INFO("gRPC serialize done: "
                                "req=%u seq=%lu slot=%u status=%d len=%u out_offset=%lu\n",
                                comp_msg->request_id, comp_msg->ring_seq, done_idx,
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

    if (state == 0 || worker >= DMESH_GRPC_SERIALIZER_THREADS)
        return;

    for (;;) {
        uint32_t cons = __atomic_load_n(&state->serializer_cons[worker], __ATOMIC_ACQUIRE);
        uint32_t prod = __atomic_load_n(&state->serializer_prod[worker], __ATOMIC_ACQUIRE);
        uint32_t slot;
        struct dpa_grpc_serialize_task *task;

        if (cons == prod) {
            if (++idle_polls >= DMESH_GRPC_SERIALIZER_IDLE_POLL_BUDGET) {
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
            __dpa_thread_window_read_inv();
            continue;
        }
        
        idle_polls = 0;
        slot = cons % DMESH_GRPC_SERIALIZER_QUEUE_DEPTH;
        task = &state->serializer_tasks[worker][slot];
        if (__atomic_load_n(&task->valid, __ATOMIC_ACQUIRE) == 0) {
            __dpa_thread_window_read_inv();
            continue;
        }

        complete_grpc_serialize_task(thread_arg, task, /*reverse=*/1);

        __atomic_store_n(&task->valid, 0U, __ATOMIC_RELEASE);
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
    // doca_dpa_dev_buf_t buf = 0;
    // doca_dpa_dev_uintptr_t dev_ptr = 0;
    // uint64_t buf_len = 0;
    // uintptr_t addr = 0;
    // DOCA_DPA_DEV_LOG_INFO("buf arr: 0x%lx\n", thread_arg->dpa_buf_arr);
    // buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, 0);
    // dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    // buf_len = doca_dpa_dev_buf_get_len(buf);
    // addr = doca_dpa_dev_buf_get_addr(buf);

    // handle_msgs(thread_arg);
    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("Starting DMA manager thread...\n");
    DOCA_DPA_DEV_LOG_INFO("DPA buffer array handle: 0x%lx, size: %u\n", thread_arg->dpa_buf_arr, thread_arg->buf_arr_size);
    DOCA_DPA_DEV_LOG_INFO("DPU mmap: %u, addr: %lu host mmap: %u\n", thread_arg->dpu_mmap, thread_arg->src_addr, thread_arg->host_mmap);

    // poll_desc_ring(thread_arg);
    poll_desc_ring_grpc(thread_arg);
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
    handle_msgs_ser(thread_arg);
    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_grpc_serializer_worker(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    // DOCA_DPA_DEV_LOG_INFO("Starting gRPC serializer worker thread: idx=%u serializer=%u\n",
    //                       thread_arg->thread_index, thread_arg->serializer_index);
    poll_grpc_serializer_queue(thread_arg);
}
