#ifndef DMESH_GRPC_DEMO_REQUEST_DPA_STUB_H
#define DMESH_GRPC_DEMO_REQUEST_DPA_STUB_H

#include "demo_request_schema.h"
#include "../../framework/common/grpc_codegen_common.h"
#include "../../framework/lowering/grpc_lowering.h"

namespace dmesh::grpc_codegen::demo_generated {

Status EmitRequestSpecialized(const RequestView &request,
                              const LoweringContext &ctx,
                              ByteBuffer *out,
                              LoweringSummary *summary);

}  // namespace dmesh::grpc_codegen::demo_generated

#endif
