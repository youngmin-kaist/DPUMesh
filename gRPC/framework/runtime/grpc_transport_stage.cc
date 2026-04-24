#include "grpc_transport_stage.h"

namespace dmesh::grpc_codegen {

Status BuildGrpcEnvelope(const ByteSpan &payload,
                         TransportFlavor flavor,
                         TransportEnvelope *envelope)
{
    if (envelope == nullptr)
        return Status::Invalid("null envelope");

    envelope->flavor = flavor;
    envelope->frame.clear();
    envelope->frame.reserve(static_cast<size_t>(payload.len) + 5U);
    envelope->frame.push_back(0);
    envelope->frame.push_back(static_cast<uint8_t>((payload.len >> 24) & 0xffU));
    envelope->frame.push_back(static_cast<uint8_t>((payload.len >> 16) & 0xffU));
    envelope->frame.push_back(static_cast<uint8_t>((payload.len >> 8) & 0xffU));
    envelope->frame.push_back(static_cast<uint8_t>(payload.len & 0xffU));
    envelope->frame.insert(envelope->frame.end(), payload.data, payload.data + payload.len);
    return Status::Ok();
}

}  // namespace dmesh::grpc_codegen
