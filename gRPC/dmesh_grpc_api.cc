#include "dmesh_grpc_api.h"

#include "host_agent.h"
#include "../DPUMesh/object.h"
#include "../DPUMesh/dpa.h"

#include <algorithm>
#include <cstring>
#include <new>
#include <vector>

struct dmesh_grpc_offload {
    GrpcProtoRuntime runtime;
};

static demo::HelloRequest
make_hello_request(const dmesh_grpc_hello_request_view &view)
{
    demo::HelloRequest req{};
    req.id_ = view.id;
    if (view.name != nullptr && view.name_len != 0)
        req.name_.assign(view.name, view.name + view.name_len);
    if (view.scores != nullptr && view.scores_count != 0)
        req.scores_.assign(view.scores, view.scores + view.scores_count);
    return req;
}

static doca_error_t
copy_encoded_batch(const std::vector<std::vector<uint8_t>> &encoded_batch,
                   const std::vector<ProtoCompletion> *cpls,
                   const uint32_t *request_ids,
                   dmesh_grpc_encoded_buf *outs,
                   uint32_t num_reqs)
{
    for (uint32_t i = 0; i < num_reqs; ++i) {
        const std::vector<uint8_t> &encoded = encoded_batch[i];
        if (outs[i].data == nullptr || outs[i].capacity < encoded.size())
            return DOCA_ERROR_NO_MEMORY;

        if (!encoded.empty())
            std::memcpy(outs[i].data, encoded.data(), encoded.size());

        outs[i].len = static_cast<uint32_t>(encoded.size());
        outs[i].request_id = request_ids[i];
        outs[i].status = 0;

        if (cpls != nullptr && i < cpls->size()) {
            outs[i].request_id = (*cpls)[i].request_id;
            outs[i].status = (*cpls)[i].status;
            outs[i].len = (*cpls)[i].encoded_len;
        }
    }

    return DOCA_SUCCESS;
}

extern "C" doca_error_t
dmesh_grpc_offload_init(struct objects *objs, uint32_t max_batch)
{
    doca_error_t result;
    dmesh_grpc_offload *ctx;

    if (objs == nullptr || objs->dpa_thread == nullptr || objs->dpa_thread->dpa == nullptr)
        return DOCA_ERROR_INVALID_VALUE;
    if (max_batch == 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (objs->grpc_offload != nullptr)
        return DOCA_SUCCESS;

    ctx = new (std::nothrow) dmesh_grpc_offload{};
    if (ctx == nullptr)
        return DOCA_ERROR_NO_MEMORY;

    result = grpc_proto_runtime_init(&ctx->runtime,
                                     objs->dpa_thread->dpa,
                                     objs->dpa_thread->thread,
                                     max_batch);
    if (result != DOCA_SUCCESS) {
        delete ctx;
        return result;
    }

    objs->grpc_offload = ctx;
    return DOCA_SUCCESS;
}

extern "C" void
dmesh_grpc_offload_destroy(struct objects *objs)
{
    dmesh_grpc_offload *ctx;

    if (objs == nullptr || objs->grpc_offload == nullptr)
        return;

    ctx = objs->grpc_offload;
    grpc_proto_runtime_destroy(&ctx->runtime);
    delete ctx;
    objs->grpc_offload = nullptr;
}

extern "C" doca_error_t
dmesh_grpc_serialize_hello(struct objects *objs,
                             const struct dmesh_grpc_hello_request_view *req,
                             uint32_t request_id,
                             struct dmesh_grpc_encoded_buf *out)
{
    std::vector<uint8_t> encoded;
    ProtoCompletion cpl{};
    demo::HelloRequest hello;
    dmesh_grpc_offload *ctx;
    doca_error_t result;

    if (objs == nullptr || req == nullptr || out == nullptr || objs->grpc_offload == nullptr)
        return DOCA_ERROR_INVALID_VALUE;

    ctx = objs->grpc_offload;
    hello = make_hello_request(*req);

    result = grpc_proto_serialize_hello(&ctx->runtime, hello, request_id, encoded, &cpl);
    if (result != DOCA_SUCCESS)
        return result;
    if (out->data == nullptr || out->capacity < encoded.size())
        return DOCA_ERROR_NO_MEMORY;

    if (!encoded.empty())
        std::memcpy(out->data, encoded.data(), encoded.size());

    out->len = static_cast<uint32_t>(encoded.size());
    out->status = cpl.status;
    out->request_id = cpl.request_id;
    return DOCA_SUCCESS;
}

extern "C" doca_error_t
dmesh_grpc_serialize_hello_batch(struct objects *objs,
                                   const struct dmesh_grpc_hello_request_view *reqs,
                                   const uint32_t *request_ids,
                                   uint32_t num_reqs,
                                   struct dmesh_grpc_encoded_buf *outs)
{
    std::vector<demo::HelloRequest> hello_reqs;
    std::vector<uint32_t> batch_request_ids;
    std::vector<std::vector<uint8_t>> encoded_batch;
    std::vector<ProtoCompletion> cpls;
    dmesh_grpc_offload *ctx;
    doca_error_t result;

    if (objs == nullptr || reqs == nullptr || request_ids == nullptr || outs == nullptr || objs->grpc_offload == nullptr)
        return DOCA_ERROR_INVALID_VALUE;
    if (num_reqs == 0)
        return DOCA_ERROR_INVALID_VALUE;

    ctx = objs->grpc_offload;
    hello_reqs.reserve(num_reqs);
    batch_request_ids.reserve(num_reqs);
    for (uint32_t i = 0; i < num_reqs; ++i) {
        hello_reqs.push_back(make_hello_request(reqs[i]));
        batch_request_ids.push_back(request_ids[i]);
    }

    result = grpc_proto_serialize_hello_batch(&ctx->runtime,
                                              hello_reqs,
                                              batch_request_ids,
                                              encoded_batch,
                                              &cpls);
    if (result != DOCA_SUCCESS)
        return result;

    return copy_encoded_batch(encoded_batch, &cpls, request_ids, outs, num_reqs);
}
