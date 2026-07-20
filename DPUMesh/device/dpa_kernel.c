#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"
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

__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);

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
static void handle_dpu_msg(struct dpa_thread_arg *thread_arg, const struct comch_msg *msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    // doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_completion_element_t comp;
    uint64_t start, end;

    switch(msg->type) {
        case COMCH_MSG_TYPE_DMA_REQ:
            struct comch_dma_req_msg *dma_msg = (struct comch_dma_req_msg *)msg;
            if (dma_msg->dpa_producer)
                producer = dma_msg->dpa_producer;
            // producer = dma_msg->dpa_producer;
            DOCA_DPA_DEV_LOG_INFO("Received DMA REQ msg from host: producer=0x%lx, src_mmap=%u, dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u\n",
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
                                    "test_dma_imm",
                                    sizeof("test_dma_imm"),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            break;
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg->type);
            break;
    }
}

static void handle_msgs(struct dpa_thread_arg *thread_arg)
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
#define CONSUMER_HEAD_PUBLISH_BATCH 1
#define DMA_RING_LOG_INTERVAL 1024
/* Max descriptors / bytes coalesced into a single DMA copy + completion
 * message. HARD CONSTRAINT: single DPA DMA transfers above 8192B do not work
 * on this platform, so a coalesced copy must never exceed 8192B (coalescing
 * therefore only helps messages smaller than 8KB). */
#define DMA_BATCH_MAX_DESCS 8
#define DMA_BATCH_MAX_BYTES 8192
/* Ring the doorbell only every N submissions (and at the end of a burst);
 * intermediate copies are enqueued without FLUSH. */
#define DMA_FLUSH_BATCH 32

static void poll_desc_ring(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    struct comch_dma_comp_msg msg = {0};
    doca_dpa_dev_uintptr_t dev_ptr;
    doca_dpa_dev_buf_t buf;
    struct dma_ring_ctrl *ctrl;
    struct dma_desc *desc;
    uint64_t producer_tail;
    uint64_t consumer_head;
    uint64_t last_published_head;

    uint32_t ring_size = thread_arg->buf_arr_size;
    uint32_t ring_mask = ring_size - 1;
    uint32_t buf_size = thread_arg->buf_size;
    uint32_t unflushed = 0;

    DOCA_DPA_DEV_LOG_INFO("Polling descriptor ring with size %u, buf_size: %u\n", ring_size, buf_size);
    
    buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, 0);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    ctrl = (struct dma_ring_ctrl *)dev_ptr;

    consumer_head = ctrl->consumer_head;
    last_published_head = consumer_head;
    DOCA_DPA_DEV_LOG_INFO("DPA ring init: producer_tail=%lu, consumer_head=%lu\n",
                          ctrl->producer_tail, consumer_head);

    /* polling descriptor ring in host memory */
    while (1) {

        __dpa_thread_window_read_inv();
        buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, 0);
        dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
        ctrl = (struct dma_ring_ctrl *)dev_ptr;
        consumer_head = ctrl->consumer_head;
        producer_tail = ctrl->producer_tail;

        while (consumer_head < producer_tail) {
            uint64_t batch_src;
            uint32_t batch_len, batch_cnt;
            uint32_t dst_room = buf_size - (uint32_t)thread_arg->pos;

            buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, (consumer_head & ring_mask) + 1);
            dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
            desc = (struct dma_desc *)dev_ptr;

            batch_src = desc->addr;
            batch_len = (uint32_t)desc->size;
            batch_cnt = 1;

            /* Coalesce contiguous descriptors into one DMA copy: one WQE, one
             * doorbell and one completion message then cover the whole batch,
             * amortizing the fixed per-operation cost. The host writes
             * messages back-to-back in sndbuf, so descriptors are contiguous
             * except at the buffer wrap; the batch also may not cross the
             * staging-buffer wrap on the destination side. */
            // while (batch_cnt < DMA_BATCH_MAX_DESCS &&
            //        consumer_head + batch_cnt < producer_tail) {
            //     buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr,
            //                                          ((consumer_head + batch_cnt) & ring_mask) + 1);
            //     dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
            //     desc = (struct dma_desc *)dev_ptr;

            //     if (desc->addr != batch_src + batch_len)   /* source discontinuity (wrap) */
            //         break;
            //     if (batch_len + desc->size > dst_room)     /* staging wrap on destination */
            //         break;
            //     if (batch_len + desc->size > DMA_BATCH_MAX_BYTES)
            //         break;
            //     batch_len += (uint32_t)desc->size;
            //     batch_cnt++;
            // }

            /* if consumer is empty, wait */
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
            }

            msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
            msg.pos = thread_arg->pos;
            msg.length = batch_len;
            msg.count = batch_cnt;

            /* Batch doorbells: only every DMA_FLUSH_BATCH submissions - or on
             * the last pending batch of this burst - carries FLUSH. */
            {
                uint64_t submit_flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS;

                unflushed++;
                if (unflushed >= DMA_FLUSH_BATCH ||
                    consumer_head + batch_cnt >= producer_tail) {
                    submit_flags |= DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH;
                    unflushed = 0;
                }

                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            /*consumer_id=*/1,
                                            thread_arg->dpu_mmap,
                                            thread_arg->src_addr + thread_arg->pos,
                                            thread_arg->host_mmap,
                                            batch_src,
                                            batch_len,
                                            (uint8_t *)&msg,
                                            sizeof(struct comch_dma_comp_msg),
                                            submit_flags);
            }

            thread_arg->pos += batch_len;
            if (thread_arg->pos >= buf_size) {
                thread_arg->pos = 0;
            }

            consumer_head += batch_cnt;
        }

        if (consumer_head - last_published_head >= CONSUMER_HEAD_PUBLISH_BATCH) {
            ctrl->consumer_head = consumer_head;
            __dpa_thread_window_writeback();
            last_published_head = consumer_head;
        }
    }

    // while (consumer_head < ctrl->producer_tail) {
    //     desc_idx = (consumer_head & ring_mask) + 1;
    //     buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, desc_idx);
    //     dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    //     desc = (struct dma_desc *)dev_ptr;
    //     DOCA_DPA_DEV_LOG_INFO("Read DMA desc: idx=%lu, addr=0x%lx, size=%lu\n", desc->idx, desc->addr, desc->size);
        
    //     consumer_head++;
    // }

    // while (1) {
    //     __dpa_thread_window_read_inv();
    //     producer_tail = ctrl->producer_tail;
    //     while (consumer_head == producer_tail) {
    //         if (consumer_head != last_published_head) {
    //             ctrl->consumer_head = consumer_head;
    //             __dpa_thread_window_writeback();
    //             last_published_head = consumer_head;
    //         }
    //         __dpa_thread_window_read_inv();
    //         producer_tail = ctrl->producer_tail;
    //     }

    //     buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, (consumer_head & ring_mask) + 1);
    //     dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    //     __dpa_thread_window_read_inv();
    //     desc = (struct dma_desc *)dev_ptr;

    //     /* if consumer is empty, wait */
    //     while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
    //     }

    //     // DOCA_DPA_DEV_LOG_INFO("Read DMA desc: idx=%lu, addr=0x%lx, size=%lu\n", desc->idx, desc->addr, desc->size);
    //     msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    //     msg.pos = thread_arg->pos;
    //     msg.length = desc->size;

    //     doca_dpa_dev_comch_producer_dma_copy(producer,
    //                                 /*consumer_id=*/1,
    //                                 thread_arg->dpu_mmap,
    //                                 thread_arg->src_addr + thread_arg->pos,
    //                                 thread_arg->host_mmap,
    //                                 desc->addr,
    //                                 desc->size,
    //                                 (uint8_t *)&msg,
    //                                 sizeof(struct comch_dma_comp_msg),
    //                                 DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | 
    //                                 DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    //     thread_arg->pos += desc->size;
    //     if (thread_arg->pos >= buf_size) {
    //         thread_arg->pos = 0;
    //     }

    //     consumer_head++;
    //     if (consumer_head - last_published_head >= CONSUMER_HEAD_PUBLISH_BATCH) {
    //         ctrl->consumer_head = consumer_head;
    //         __dpa_thread_window_writeback();
    //         last_published_head = consumer_head;
    //     }

    //     if (consumer_head - last_logged_head >= DMA_RING_LOG_INTERVAL) {
    //         // DOCA_DPA_DEV_LOG_INFO("DPA ring consume: producer_tail=%lu, consumer_head=%lu\n",
    //         //                       producer_tail, consumer_head);
    //         last_logged_head = consumer_head;
    //     }
    // }
}

/*
 * producer_dma_copy microbenchmark: copy bench_msg_size bytes from the host
 * sndbuf to the DPU staging buffer bench_num_ops times, straight from this DPA
 * thread. Throughput mode pipelines copies (credit-gated, doorbell every
 * DMA_FLUSH_BATCH); latency mode serializes: FLUSH each copy and poll the
 * producer completion before issuing the next. Results are reported in
 * __dpa_thread_time() ticks; the DPU-side recv counters give the wall-clock
 * cross-check.
 */
static void run_dma_copy_bench(struct dpa_thread_arg *a)
{
    doca_dpa_dev_comch_producer_t producer = a->dpa_producer;
    doca_dpa_dev_completion_element_t comp;
    struct comch_dma_comp_msg msg = {0};
    uint32_t size = a->bench_msg_size;
    uint32_t n = a->bench_num_ops;
    uint64_t dst_pos = 0, src_pos = 0;
    uint64_t t0, t1;
    uint32_t i;

    msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    msg.length = size;
    msg.count = 1;

    DOCA_DPA_DEV_LOG_INFO("BENCH start: mode=%u size=%u ops=%u host=0x%lx/%u dpu=0x%lx/%u\n",
                          a->bench_mode, size, n,
                          a->bench_host_addr, a->bench_host_size,
                          a->src_addr, a->buf_size);

    if (a->bench_mode == 1) {
        /* throughput: keep the pipe full, doorbell in batches; the final copy
         * carries a completion report so t1 covers full drain */
        t0 = __dpa_thread_time();
        for (i = 0; i < n; i++) {
            uint64_t flags;

            if (i == n - 1)
                flags = DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH; /* reported */
            else if ((i & (DMA_FLUSH_BATCH - 1)) == DMA_FLUSH_BATCH - 1)
                flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS |
                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH;
            else
                flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS;

            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
            }

            msg.pos = (uint32_t)dst_pos;
            doca_dpa_dev_comch_producer_dma_copy(producer,
                                                 /*consumer_id=*/1,
                                                 a->dpu_mmap,
                                                 a->src_addr + dst_pos,
                                                 a->host_mmap,
                                                 a->bench_host_addr + src_pos,
                                                 size,
                                                 (uint8_t *)&msg,
                                                 sizeof(struct comch_dma_comp_msg),
                                                 flags);

            dst_pos += size;
            if (dst_pos + size > a->buf_size)
                dst_pos = 0;
            src_pos += size;
            if (src_pos + size > a->bench_host_size)
                src_pos = 0;
        }
        while (doca_dpa_dev_get_completion(a->dpa_producer_comp, &comp) == 0) {
        }
        t1 = __dpa_thread_time();
        doca_dpa_dev_completion_ack(a->dpa_producer_comp, 1);
        DOCA_DPA_DEV_LOG_INFO("BENCH_TPUT size=%u ops=%u ticks=%lu\n", size, n, t1 - t0);
    } else {
        /* latency: one copy at a time, completion-polled. Per-op deltas use
         * the cycle counter; the 1 MHz thread timer over the whole run
         * calibrates cycles -> ns. */
        uint64_t total = 0, tmin = (uint64_t)-1, tmax = 0, d;
        uint64_t run_t0 = __dpa_thread_time();

        for (i = 0; i < n; i++) {
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
            }

            t0 = __dpa_thread_cycles();
            msg.pos = (uint32_t)dst_pos;
            doca_dpa_dev_comch_producer_dma_copy(producer,
                                                 /*consumer_id=*/1,
                                                 a->dpu_mmap,
                                                 a->src_addr + dst_pos,
                                                 a->host_mmap,
                                                 a->bench_host_addr + src_pos,
                                                 size,
                                                 (uint8_t *)&msg,
                                                 sizeof(struct comch_dma_comp_msg),
                                                 DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            while (doca_dpa_dev_get_completion(a->dpa_producer_comp, &comp) == 0) {
            }
            t1 = __dpa_thread_cycles();
            doca_dpa_dev_completion_ack(a->dpa_producer_comp, 1);

            d = t1 - t0;
            total += d;
            if (d < tmin)
                tmin = d;
            if (d > tmax)
                tmax = d;

            dst_pos += size;
            if (dst_pos + size > a->buf_size)
                dst_pos = 0;
            src_pos += size;
            if (src_pos + size > a->bench_host_size)
                src_pos = 0;
        }
        DOCA_DPA_DEV_LOG_INFO("BENCH_LAT size=%u ops=%u total_cycles=%lu min=%lu max=%lu run_us=%lu\n",
                              size, n, total, tmin, tmax,
                              __dpa_thread_time() - run_t0);
    }
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("Starting DMA manager thread...\n");
    DOCA_DPA_DEV_LOG_INFO("DPA buffer array handle: 0x%lx, size: %u\n", thread_arg->dpa_buf_arr, thread_arg->buf_arr_size);
    DOCA_DPA_DEV_LOG_INFO("DPU mmap: %u, addr: %p host mmap: %u\n", thread_arg->dpu_mmap, thread_arg->src_addr, thread_arg->host_mmap);

    if (thread_arg->bench_mode != 0) {
        run_dma_copy_bench(thread_arg);
        /* run once per thread activation only */
        thread_arg->bench_mode = 0;
        doca_dpa_dev_thread_reschedule();
    }

    /* bench-only launch (host-driven): no descriptor ring to poll */
    if (thread_arg->dpa_buf_arr == 0)
        doca_dpa_dev_thread_reschedule();

    poll_desc_ring(thread_arg);

    doca_dpa_dev_thread_reschedule();
}
