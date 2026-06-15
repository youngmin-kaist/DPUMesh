#pragma once

#include "proto_meta.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_ENCODED_SIZE 8192U

typedef struct {
    uint32_t specialized_attempts;
    uint32_t specialized_hits;
    uint32_t generic_fallbacks;
} GrpcWireEncodeStats;

typedef int (*GrpcWireCopySubmitFn)(void *ctx,
                                    uint64_t dst_addr,
                                    uint64_t src_addr,
                                    uint32_t len);

void grpc_wire_encode_stats_reset(GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one(const ProtoSchemaBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one_reverse(const ProtoSchemaBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one_copy(const ProtoTask *task,
                                 uint32_t flat_len,
                                 ProtoCompletion *cpl,
                                 GrpcWireCopySubmitFn copy_submit,
                                 void *copy_ctx);

#ifdef __cplusplus
}
#endif
