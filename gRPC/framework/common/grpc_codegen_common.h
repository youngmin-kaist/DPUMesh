#ifndef DMESH_GRPC_CODEGEN_COMMON_H
#define DMESH_GRPC_CODEGEN_COMMON_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dmesh::grpc_codegen {

enum class StatusCode : int32_t {
    kOk = 0,
    kInvalidArgument = 1,
    kUnsupported = 2,
    kResourceExhausted = 3,
    kInternal = 4,
};

struct Status {
    StatusCode code;
    std::string message;

    static Status Ok() { return {StatusCode::kOk, {}}; }
    static Status Invalid(std::string msg) { return {StatusCode::kInvalidArgument, std::move(msg)}; }
    static Status Unsupported(std::string msg) { return {StatusCode::kUnsupported, std::move(msg)}; }
    static Status Resource(std::string msg) { return {StatusCode::kResourceExhausted, std::move(msg)}; }
    static Status Internal(std::string msg) { return {StatusCode::kInternal, std::move(msg)}; }
    bool ok() const { return code == StatusCode::kOk; }
};

enum class WireType : uint8_t {
    kVarint = 0,
    kFixed64 = 1,
    kLengthDelimited = 2,
    kFixed32 = 5,
};

enum class FieldKind : uint8_t {
    kUint64,
    kUint32,
    kString,
    kBytes,
    kMessage,
    kPackedUint32,
    kMapEntry,
};

enum class BackendKind : uint8_t {
    kGenericPlan,
    kSpecializedCodegen,
};

enum class HybridRefKind : uint8_t {
    kCopied,
    kZeroCopy,
};

enum class TransportFlavor : uint8_t {
    kGrpcPrefixOnly,
    kGrpcPrefixWithHttp2Placeholder,
};

struct BoundsConfig {
    uint32_t max_nesting_depth = 8;
    uint32_t max_repeated_count = 4096;
    uint32_t max_map_entries = 1024;
    uint32_t max_encoded_size = 1U << 20;
};

struct ByteSpan {
    const uint8_t *data = nullptr;
    uint32_t len = 0;
};

using ByteBuffer = std::vector<uint8_t>;

}  // namespace dmesh::grpc_codegen

#endif
