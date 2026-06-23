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

struct DmeshGrpcRef {
    uint32_t offset;
    uint32_t len;
};

struct DmeshGrpcU32ArrayRef {
    uint32_t offset;
    uint32_t count;
};

/*
 * ABI mirror of struct dmesh_grpc_hello_flat from dpa_common.h. Keep this file
 * free of DOCA host headers so it can be compiled by DPACC for device code.
 */
struct DmeshGrpcHelloFlat {
    uint8_t reserved[3];
    uint8_t compressed;
    uint32_t msg_len;
    uint64_t id;
    struct DmeshGrpcRef name;
    struct DmeshGrpcU32ArrayRef scores;
};

typedef int (*GrpcWireCopySubmitFn)(void *ctx,
                                    uint64_t dst_addr,
                                    uint64_t src_addr,
                                    uint32_t len);

void grpc_wire_encode_stats_reset(GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one(const ProtoSchemaBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one_copy(const ProtoTask *task,
                                 uint32_t flat_len,
                                 ProtoCompletion *cpl,
                                 GrpcWireCopySubmitFn copy_submit,
                                 void *copy_ctx);


int grpc_wire_serialize_one_dmesh_flat(const ProtoSchemaBlob *blob,
                                       const ProtoTask *task,
                                       ProtoCompletion *cpl,
                                       GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one_reverse(const ProtoSchemaBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one_reverse_dmesh_flat(const ProtoSchemaBlob *blob,
                                               const ProtoTask *task,
                                               ProtoCompletion *cpl,
                                               GrpcWireEncodeStats *stats);


#ifdef __cplusplus
}
#endif
