#ifndef DMESH_GRPC_CODEGEN_VERIFY_H
#define DMESH_GRPC_CODEGEN_VERIFY_H

#include "../codegen/generated/demo_request_schema.h"
#include "../framework/common/grpc_codegen_common.h"

namespace dmesh::grpc_codegen::tests {

enum class ReferenceBackendKind : uint8_t {
    kFallbackHostReference,
    kStandardProtobufAdapter,
};

Status SerializeWithHostReferenceFallback(const demo_generated::RequestView &request,
                                          ByteBuffer *out);
Status SerializeWithStandardProtobufAdapter(const demo_generated::RequestView &request,
                                            ByteBuffer *out);
Status SerializeWithPreferredReference(const demo_generated::RequestView &request,
                                       ReferenceBackendKind preferred,
                                       ByteBuffer *out,
                                       ReferenceBackendKind *used_backend);

bool BuffersEqual(const ByteBuffer &a, const ByteBuffer &b);
std::string HexPreview(const ByteBuffer &buf, size_t max_bytes);

demo_generated::RequestView MakeSampleRequest();

}  // namespace dmesh::grpc_codegen::tests

#endif
