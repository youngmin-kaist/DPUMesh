#ifndef DMESH_GRPC_EXEC_PLAN_H
#define DMESH_GRPC_EXEC_PLAN_H

#include "../schema/grpc_schema_desc.h"
#include <variant>

namespace dmesh::grpc_codegen {

struct DmaSafeBufferMeta {
    bool dma_safe = false;
    bool ownership_tracked = false;
    uint64_t ownership_cookie = 0;
    uintptr_t recover_token = 0;
    const char *origin = nullptr;
};

struct InlineCopyRef {
    ByteBuffer bytes;
};

struct ExternalZeroCopyRef {
    ByteSpan span;
    DmaSafeBufferMeta meta;
};

using HybridFieldRef = std::variant<InlineCopyRef, ExternalZeroCopyRef>;

struct ScalarValue {
    uint64_t u64 = 0;
};

struct EmitOp {
    enum class Kind : uint8_t {
        kVarint,
        kLengthDelimited,
        kStartSubMessage,
        kEndSubMessage,
        kPackedVarints,
    } kind = Kind::kVarint;

    uint32_t field_no = 0;
    FieldKind field_kind = FieldKind::kUint64;
    ScalarValue scalar{};
    HybridFieldRef hybrid{};
    uint32_t nested_plan_index = UINT32_MAX;
    std::vector<uint64_t> packed_values;
    const MessageDesc *submessage_desc = nullptr;
};

struct ExecPlan {
    const MessageDesc *message_desc = nullptr;
    BackendKind backend = BackendKind::kGenericPlan;
    uint64_t schema_hash = 0;
    uint32_t schema_version = 0;
    BoundsConfig bounds{};
    std::vector<EmitOp> ops;
    std::vector<ExecPlan> nested_plans;
    size_t zero_copy_field_count = 0;
    size_t copied_field_count = 0;
};

}  // namespace dmesh::grpc_codegen

#endif
