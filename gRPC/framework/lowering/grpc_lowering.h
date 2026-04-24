#ifndef DMESH_GRPC_LOWERING_H
#define DMESH_GRPC_LOWERING_H

#include "../policy/grpc_selective_policy.h"

namespace dmesh::grpc_codegen {

struct LoweringContext {
    const SelectiveOffloadPolicy *policy = nullptr;
    SelectiveOffloadContext selective_ctx{};
    BoundsConfig bounds{};
};

struct LoweringSummary {
    size_t copied_fields = 0;
    size_t zero_copy_fields = 0;
};

}  // namespace dmesh::grpc_codegen

#endif
