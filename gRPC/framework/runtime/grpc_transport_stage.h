#ifndef DMESH_GRPC_TRANSPORT_STAGE_H
#define DMESH_GRPC_TRANSPORT_STAGE_H

#include "../common/grpc_codegen_common.h"

namespace dmesh::grpc_codegen {

struct TransportEnvelope {
    TransportFlavor flavor = TransportFlavor::kGrpcPrefixOnly;
    ByteBuffer frame;
};

Status BuildGrpcEnvelope(const ByteSpan &payload,
                         TransportFlavor flavor,
                         TransportEnvelope *envelope);

}  // namespace dmesh::grpc_codegen

#endif
