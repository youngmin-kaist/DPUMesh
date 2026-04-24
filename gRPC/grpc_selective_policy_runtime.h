#pragma once

#include <stdint.h>

typedef enum {
    GRPC_FIELD_PLACEMENT_COPY = 0,
    GRPC_FIELD_PLACEMENT_ZERO_COPY_CANDIDATE = 1,
    GRPC_FIELD_PLACEMENT_COPY_FALLBACK_UNSAFE = 2,
} GrpcFieldPlacementKind;

typedef struct {
    GrpcFieldPlacementKind kind;
    uint32_t threshold;
    uint32_t len;
    uint8_t dma_safe;
} GrpcFieldPlacementDecision;

typedef struct {
    uint32_t string_threshold;
    uint32_t bytes_threshold;
} GrpcSelectiveOffloadPolicyConfig;

typedef struct {
    uint32_t copied_fields;
    uint32_t zero_copy_candidate_fields;
    uint32_t copy_fallback_unsafe_fields;
} GrpcSelectiveOffloadPolicyStats;

GrpcFieldPlacementDecision grpc_selective_policy_decide_string(const GrpcSelectiveOffloadPolicyConfig *cfg,
                                                               uint32_t len,
                                                               uint8_t dma_safe);

void grpc_selective_policy_stats_reset(GrpcSelectiveOffloadPolicyStats *stats);

void grpc_selective_policy_stats_record(GrpcSelectiveOffloadPolicyStats *stats,
                                        const GrpcFieldPlacementDecision *decision);
