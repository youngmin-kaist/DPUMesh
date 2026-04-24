#pragma once

#include <doca_error.h>

#include <stdint.h>

struct objects;

#ifdef __cplusplus
extern "C" {
#endif

struct dmesh_grpc_hello_request_view {
    uint64_t id;
    const char *name;
    uint32_t name_len;
    const uint32_t *scores;
    uint32_t scores_count;
};

struct dmesh_grpc_encoded_buf {
    uint8_t *data;
    uint32_t capacity;
    uint32_t len;
    int32_t status;
    uint32_t request_id;
};

doca_error_t
dmesh_grpc_offload_init(struct objects *objs, uint32_t max_batch);

void
dmesh_grpc_offload_destroy(struct objects *objs);

doca_error_t
dmesh_grpc_serialize_hello(struct objects *objs,
                             const struct dmesh_grpc_hello_request_view *req,
                             uint32_t request_id,
                             struct dmesh_grpc_encoded_buf *out);

doca_error_t
dmesh_grpc_serialize_hello_batch(struct objects *objs,
                                   const struct dmesh_grpc_hello_request_view *reqs,
                                   const uint32_t *request_ids,
                                   uint32_t num_reqs,
                                   struct dmesh_grpc_encoded_buf *outs);

#ifdef __cplusplus
}
#endif
