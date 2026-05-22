#pragma once

#include "proto_meta.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint32_t specialized_attempts;
    uint32_t specialized_hits;
    uint32_t generic_fallbacks;
} GrpcWireEncodeStats;

void grpc_wire_encode_stats_reset(GrpcWireEncodeStats *stats);

int grpc_wire_serialize_one(const ProtoDescBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats);

#ifdef __cplusplus
}
#endif
