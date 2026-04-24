#ifndef DMESH_GRPC_DMA_SAFETY_H
#define DMESH_GRPC_DMA_SAFETY_H

#include "../ir/grpc_exec_plan.h"

namespace dmesh::grpc_codegen {

struct BufferCandidate {
    ByteSpan span{};
    bool dma_safe = false;
    bool ownership_tracked = false;
    uint64_t ownership_cookie = 0;
    uintptr_t recover_token = 0;
    const char *origin = nullptr;
};

inline DmaSafeBufferMeta ToDmaMeta(const BufferCandidate &candidate)
{
    DmaSafeBufferMeta meta{};
    meta.dma_safe = candidate.dma_safe;
    meta.ownership_tracked = candidate.ownership_tracked;
    meta.ownership_cookie = candidate.ownership_cookie;
    meta.recover_token = candidate.recover_token;
    meta.origin = candidate.origin;
    return meta;
}

}  // namespace dmesh::grpc_codegen

#endif
