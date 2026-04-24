#include "demo_request_schema.h"

namespace dmesh::grpc_codegen::demo_generated {
namespace {

const FieldDesc kInnerFields[] = {
    {1, "note", FieldKind::kString, WireType::kLengthDelimited, false, false, true, false, UINT32_MAX, nullptr},
    {2, "ts", FieldKind::kUint64, WireType::kVarint, false, false, false, false, UINT32_MAX, nullptr},
};

const FieldDesc kAttrEntryValueFields[] = {
    {1, "num", FieldKind::kUint64, WireType::kVarint, false, false, false, false, 0, nullptr},
    {2, "text", FieldKind::kString, WireType::kLengthDelimited, false, false, false, false, 0, nullptr},
};

const FieldDesc kAttrEntryFields[] = {
    {1, "key", FieldKind::kString, WireType::kLengthDelimited, false, false, false, false, UINT32_MAX, nullptr},
    {2, "value", FieldKind::kMessage, WireType::kLengthDelimited, false, false, false, false, UINT32_MAX, "demo.AttrEntryValue"},
};

const FieldDesc kRequestFields[] = {
    {1, "id", FieldKind::kUint64, WireType::kVarint, false, false, false, false, UINT32_MAX, nullptr},
    {2, "name", FieldKind::kString, WireType::kLengthDelimited, false, false, true, false, UINT32_MAX, nullptr},
    {3, "inner", FieldKind::kMessage, WireType::kLengthDelimited, false, false, false, false, UINT32_MAX, "demo.Inner"},
    {4, "attrs", FieldKind::kMapEntry, WireType::kLengthDelimited, true, false, false, true, UINT32_MAX, "demo.Request.AttrsEntry"},
    {5, "raw", FieldKind::kBytes, WireType::kLengthDelimited, false, false, false, false, 1, nullptr},
    {6, "nested", FieldKind::kMessage, WireType::kLengthDelimited, false, false, false, false, 1, "demo.Inner"},
    {7, "scores", FieldKind::kPackedUint32, WireType::kLengthDelimited, true, true, false, false, UINT32_MAX, nullptr},
};

const MessageDesc kInnerDesc = {"demo.Inner", kSchemaVersion, kSchemaHash ^ 0x10ULL, kInnerFields, 2};
const MessageDesc kAttrEntryValueDesc = {"demo.AttrEntryValue", kSchemaVersion, kSchemaHash ^ 0x20ULL, kAttrEntryValueFields, 2};
const MessageDesc kAttrEntryDesc = {"demo.Request.AttrsEntry", kSchemaVersion, kSchemaHash ^ 0x30ULL, kAttrEntryFields, 2};
const MessageDesc kRequestDesc = {"demo.Request", kSchemaVersion, kSchemaHash, kRequestFields, 7};

}  // namespace

BufferCandidate PayloadBufferInput::AsCandidate() const
{
    BufferCandidate candidate{};
    candidate.span.data = bytes.data();
    candidate.span.len = static_cast<uint32_t>(bytes.size());
    candidate.dma_safe = dma_safe;
    candidate.ownership_tracked = ownership_tracked;
    candidate.ownership_cookie = ownership_cookie;
    candidate.recover_token = recover_token;
    candidate.origin = origin.c_str();
    return candidate;
}

std::string PayloadBufferInput::AsString() const
{
    return std::string(bytes.begin(), bytes.end());
}

const MessageDesc *GetInnerDesc() { return &kInnerDesc; }
const MessageDesc *GetAttrEntryValueDesc() { return &kAttrEntryValueDesc; }
const MessageDesc *GetRequestDesc() { return &kRequestDesc; }
const MessageDesc *GetAttrEntryDesc() { return &kAttrEntryDesc; }

void RegisterDemoRequestSchemas(SchemaRegistry *registry)
{
    if (registry == nullptr)
        return;
    registry->Register(&kInnerDesc);
    registry->Register(&kAttrEntryValueDesc);
    registry->Register(&kAttrEntryDesc);
    registry->Register(&kRequestDesc);
}

}  // namespace dmesh::grpc_codegen::demo_generated
