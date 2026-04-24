#include "doca_dpa_dev.h"
#include "dpaintrin.h"

#include "grpc_wire_encode.h"

#include <stdint.h>

__dpa_rpc__ uint64_t grpc_dpa_serialize_batch_rpc(uint64_t worker_arg_addr, uint32_t num_tasks)
{
    const GrpcDpaWorkerArg *arg = (const GrpcDpaWorkerArg *)(uintptr_t)worker_arg_addr;
    const ProtoDescBlob *blob = (const ProtoDescBlob *)(uintptr_t)arg->desc_blob_addr;
    const ProtoTask *tasks = (const ProtoTask *)(uintptr_t)arg->task_array_addr;
    ProtoCompletion *cpls = (ProtoCompletion *)(uintptr_t)arg->completion_array_addr;
    uint32_t i;

    if (num_tasks > arg->max_batch)
        return 1;

    for (i = 0; i < num_tasks; ++i)
        (void)grpc_wire_serialize_one(blob, &tasks[i], &cpls[i], NULL);

    return 0;
}

__dpa_global__ void grpc_dpa_worker_main(uint64_t arg_addr)
{
    const GrpcDpaWorkerArg *arg = (const GrpcDpaWorkerArg *)(uintptr_t)arg_addr;
    const ProtoDescBlob *blob = (const ProtoDescBlob *)(uintptr_t)arg->desc_blob_addr;
    GrpcRingCtrl *ctrl = (GrpcRingCtrl *)(uintptr_t)arg->ring_ctrl_addr;
    GrpcReqDesc *req_ring = (GrpcReqDesc *)(uintptr_t)arg->req_ring_addr;
    GrpcCplDesc *cpl_ring = (GrpcCplDesc *)(uintptr_t)arg->cpl_ring_addr;
    uint32_t depth = arg->ring_depth;
    uint32_t burst_budget = arg->max_batch;

    if (ctrl == NULL || req_ring == NULL || cpl_ring == NULL || depth == 0U)
        return;
    if (burst_budget == 0U)
        burst_budget = 1U;

    for (;;) {
        uint32_t processed = 0U;

        __dpa_thread_window_read_inv();
        if (ctrl->shutdown != 0U)
            break;

        while (processed < burst_budget) {
            uint32_t req_head = ctrl->req_head;
            uint32_t req_tail = ctrl->req_tail;
            uint32_t cpl_tail = ctrl->cpl_tail;
            uint32_t next_cpl_tail = (cpl_tail + 1U) % depth;
            GrpcReqDesc *r;
            GrpcCplDesc *c;
            ProtoTask task = {0};
            ProtoCompletion pc = {0};

            if (req_head == req_tail)
                break;
            if (next_cpl_tail == ctrl->cpl_head)
                break;

            r = &req_ring[req_head];
            c = &cpl_ring[cpl_tail];
            if (r->valid == 0U)
                break;

            task.desc_id = r->desc_id;
            task.request_id = r->request_id;
            task.dpa_msg_addr = arg->msg_pool_addr + ((uint64_t)r->msg_slot * arg->msg_slot_size);
            task.dpa_out_addr = arg->out_pool_addr + ((uint64_t)r->out_slot * arg->out_slot_size);
            task.dpa_out_cap = r->out_cap;

            (void)grpc_wire_serialize_one(blob, &task, &pc, NULL);

            c->request_id = pc.request_id;
            c->encoded_len = pc.encoded_len;
            c->status = pc.status;
            c->out_slot = r->out_slot;
            c->valid = 1U;

            r->valid = 0U;
            ctrl->req_head = (req_head + 1U) % depth;
            ctrl->cpl_tail = next_cpl_tail;
            ++processed;
        }

        doca_dpa_dev_thread_reschedule();
    }
}
