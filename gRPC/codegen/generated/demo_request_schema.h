#ifndef DMESH_GRPC_DEMO_REQUEST_SCHEMA_H
#define DMESH_GRPC_DEMO_REQUEST_SCHEMA_H

#include "../../framework/schema/grpc_schema_registry.h"
#include "../../framework/policy/grpc_dma_safety.h"

#include <string>
#include <utility>
#include <vector>

namespace dmesh::grpc_codegen::demo_generated {

inline constexpr uint32_t kSchemaVersion = 1;
inline constexpr uint64_t kSchemaHash = 0x4450554d45534831ULL;

struct PayloadBufferInput {
    std::vector<uint8_t> bytes;
    bool dma_safe = false;
    bool ownership_tracked = false;
    uint64_t ownership_cookie = 0;
    uintptr_t recover_token = 0;
    std::string origin = "host";

    BufferCandidate AsCandidate() const;
    std::string AsString() const;
};

struct InnerView {
    bool has_note = false;
    PayloadBufferInput note;
    uint64_t ts = 0;
};

enum class AttrEntryValueCase : uint32_t {
    kNone = 0,
    kNum = 1,
    kText = 2,
};

struct AttrEntryValueView {
    AttrEntryValueCase kind = AttrEntryValueCase::kNone;
    uint64_t num = 0;
    PayloadBufferInput text;
};

enum class RequestPayloadCase : uint32_t {
    kNone = 0,
    kRaw = 5,
    kNested = 6,
};

struct RequestView {
    uint64_t id = 0;
    bool has_name = false;
    PayloadBufferInput name;
    InnerView inner;
    std::vector<std::pair<PayloadBufferInput, AttrEntryValueView>> attrs;
    RequestPayloadCase payload_case = RequestPayloadCase::kNone;
    PayloadBufferInput raw;
    InnerView nested;
    std::vector<uint32_t> scores;
};

const MessageDesc *GetInnerDesc();
const MessageDesc *GetAttrEntryValueDesc();
const MessageDesc *GetRequestDesc();
const MessageDesc *GetAttrEntryDesc();
void RegisterDemoRequestSchemas(SchemaRegistry *registry);

}  // namespace dmesh::grpc_codegen::demo_generated

#endif
