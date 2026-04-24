#include "grpc_selective_policy_runtime.h"

GrpcFieldPlacementDecision grpc_selective_policy_decide_string(const GrpcSelectiveOffloadPolicyConfig *cfg,
                                                               uint32_t len,
                                                               uint8_t dma_safe)
{
    GrpcFieldPlacementDecision decision{};

    decision.kind = GRPC_FIELD_PLACEMENT_COPY;
    decision.threshold = (cfg != nullptr) ? cfg->string_threshold : 0U;
    decision.len = len;
    decision.dma_safe = dma_safe;

    if (cfg == nullptr)
        return decision;

    if (len < cfg->string_threshold) {
        decision.kind = GRPC_FIELD_PLACEMENT_COPY;
    } else if (dma_safe != 0U) {
        decision.kind = GRPC_FIELD_PLACEMENT_ZERO_COPY_CANDIDATE;
    } else {
        decision.kind = GRPC_FIELD_PLACEMENT_COPY_FALLBACK_UNSAFE;
    }

    return decision;
}

void grpc_selective_policy_stats_reset(GrpcSelectiveOffloadPolicyStats *stats)
{
    if (stats == nullptr)
        return;

    stats->copied_fields = 0U;
    stats->zero_copy_candidate_fields = 0U;
    stats->copy_fallback_unsafe_fields = 0U;
}

void grpc_selective_policy_stats_record(GrpcSelectiveOffloadPolicyStats *stats,
                                        const GrpcFieldPlacementDecision *decision)
{
    if (stats == nullptr || decision == nullptr)
        return;

    switch (decision->kind) {
    case GRPC_FIELD_PLACEMENT_COPY:
        stats->copied_fields++;
        break;
    case GRPC_FIELD_PLACEMENT_ZERO_COPY_CANDIDATE:
        stats->zero_copy_candidate_fields++;
        break;
    case GRPC_FIELD_PLACEMENT_COPY_FALLBACK_UNSAFE:
        stats->copy_fallback_unsafe_fields++;
        break;
    default:
        break;
    }
}
