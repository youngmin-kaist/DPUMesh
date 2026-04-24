#include "grpc_selective_policy.h"

namespace dmesh::grpc_codegen {

SelectiveOffloadDecision ThresholdSelectiveOffloadPolicy::DecideBytesLike(
    const BufferCandidate &candidate,
    const SelectiveOffloadContext &ctx) const
{
    if (candidate.span.len < ctx.zero_copy_threshold)
        return {HybridRefKind::kCopied, "below_threshold"};
    if (!candidate.dma_safe)
        return {HybridRefKind::kCopied, "not_dma_safe"};
    return {HybridRefKind::kZeroCopy, "dma_safe_above_threshold"};
}

HybridFieldRef MakeHybridFieldRef(const BufferCandidate &candidate,
                                  const SelectiveOffloadDecision &decision)
{
    if (decision.kind == HybridRefKind::kZeroCopy) {
        ExternalZeroCopyRef ref{};
        ref.span = candidate.span;
        ref.meta = ToDmaMeta(candidate);
        return ref;
    }

    InlineCopyRef ref{};
    ref.bytes.assign(candidate.span.data, candidate.span.data + candidate.span.len);
    return ref;
}

}  // namespace dmesh::grpc_codegen
