#ifndef DMESH_GRPC_SCHEMA_DESC_H
#define DMESH_GRPC_SCHEMA_DESC_H

#include "../common/grpc_codegen_common.h"

namespace dmesh::grpc_codegen {

struct PresenceRef {
    bool *present = nullptr;
    const char *debug_name = nullptr;
};

struct OneofState {
    uint32_t oneof_index = 0;
    uint32_t active_case = 0;
};

struct FieldDesc {
    uint32_t field_no = 0;
    const char *name = nullptr;
    FieldKind kind = FieldKind::kUint64;
    WireType wire_type = WireType::kVarint;
    bool repeated = false;
    bool packed = false;
    bool optional = false;
    bool is_map = false;
    uint32_t oneof_index = UINT32_MAX;
    const char *nested_message = nullptr;
};

struct MessageDesc {
    const char *full_name = nullptr;
    uint32_t schema_version = 0;
    uint64_t schema_hash = 0;
    const FieldDesc *fields = nullptr;
    size_t field_count = 0;
};

struct MapPlan {
    const char *field_name = nullptr;
    const MessageDesc *entry_desc = nullptr;
    uint32_t max_entries = 0;
};

}  // namespace dmesh::grpc_codegen

#endif
