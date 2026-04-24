#ifndef DMESH_GRPC_SELECTIVE_POLICY_H
#define DMESH_GRPC_SELECTIVE_POLICY_H

#include "grpc_dma_safety.h"

namespace dmesh::grpc_codegen {

struct SelectiveOffloadDecision {
    HybridRefKind kind = HybridRefKind::kCopied;
    const char *reason = nullptr;
};

struct SelectiveOffloadContext {
    uint32_t zero_copy_threshold = 256;
};

class SelectiveOffloadPolicy {
public:
    virtual ~SelectiveOffloadPolicy() = default;
    virtual SelectiveOffloadDecision DecideBytesLike(const BufferCandidate &candidate,
                                                     const SelectiveOffloadContext &ctx) const = 0;
};

class ThresholdSelectiveOffloadPolicy final : public SelectiveOffloadPolicy {
public:
    SelectiveOffloadDecision DecideBytesLike(const BufferCandidate &candidate,
                                             const SelectiveOffloadContext &ctx) const override;
};

HybridFieldRef MakeHybridFieldRef(const BufferCandidate &candidate,
                                  const SelectiveOffloadDecision &decision);

}  // namespace dmesh::grpc_codegen

#endif
