#include "grpc_codegen_verify.h"
#include "../codegen/generated/demo_request_dpa_stub.h"
#include "../codegen/generated/demo_request_lowering.h"
#include "../framework/dpa/grpc_dpa_emit_iface.h"
#include "../framework/runtime/grpc_transport_stage.h"

#include <iostream>

using namespace dmesh::grpc_codegen;
using namespace dmesh::grpc_codegen::demo_generated;
using namespace dmesh::grpc_codegen::tests;

int main()
{
    ThresholdSelectiveOffloadPolicy policy;
    LoweringContext ctx{};
    LoweringSummary generic_summary{};
    LoweringSummary specialized_summary{};
    ExecPlan generic_plan{};
    ByteBuffer generic_bytes;
    ByteBuffer specialized_bytes;
    ByteBuffer reference_bytes;
    TransportEnvelope envelope{};
    ReferenceBackendKind used_backend = ReferenceBackendKind::kFallbackHostReference;
    RequestView request = MakeSampleRequest();

    ctx.policy = &policy;
    ctx.selective_ctx.zero_copy_threshold = 8;
    ctx.bounds.max_nesting_depth = 8;
    ctx.bounds.max_map_entries = 64;
    ctx.bounds.max_repeated_count = 64;
    ctx.bounds.max_encoded_size = 4096;

    Status st = BuildRequestExecPlanGeneric(request, ctx, &generic_plan, &generic_summary);
    if (!st.ok()) {
        std::cerr << "generic lowering failed: " << st.message << "\n";
        return 1;
    }

    st = EmitExecPlanGeneric(generic_plan, &generic_bytes);
    if (!st.ok()) {
        std::cerr << "generic emit failed: " << st.message << "\n";
        return 1;
    }

    st = EmitRequestSpecialized(request, ctx, &specialized_bytes, &specialized_summary);
    if (!st.ok()) {
        std::cerr << "specialized emit failed: " << st.message << "\n";
        return 1;
    }

    st = SerializeWithPreferredReference(request,
                                         ReferenceBackendKind::kStandardProtobufAdapter,
                                         &reference_bytes,
                                         &used_backend);
    if (!st.ok()) {
        std::cerr << "reference serializer failed: " << st.message << "\n";
        return 1;
    }

    if (!BuffersEqual(generic_bytes, specialized_bytes)) {
        std::cerr << "generic != specialized\n";
        std::cerr << "generic:     " << HexPreview(generic_bytes, 64) << "\n";
        std::cerr << "specialized: " << HexPreview(specialized_bytes, 64) << "\n";
        return 2;
    }

    if (!BuffersEqual(generic_bytes, reference_bytes)) {
        std::cerr << "generic != reference\n";
        std::cerr << "generic:   " << HexPreview(generic_bytes, 64) << "\n";
        std::cerr << "reference: " << HexPreview(reference_bytes, 64) << "\n";
        return 3;
    }

    st = BuildGrpcEnvelope(ByteSpan{generic_bytes.data(), static_cast<uint32_t>(generic_bytes.size())},
                           TransportFlavor::kGrpcPrefixOnly,
                           &envelope);
    if (!st.ok()) {
        std::cerr << "transport framing failed: " << st.message << "\n";
        return 4;
    }

    std::cout << "grpc_codegen_smoke: ok\n";
    std::cout << "reference_backend=" << (used_backend == ReferenceBackendKind::kFallbackHostReference ? "fallback_host_reference" : "standard_protobuf") << "\n";
    std::cout << "generic_zero_copy_fields=" << generic_summary.zero_copy_fields << " copied_fields=" << generic_summary.copied_fields << "\n";
    std::cout << "encoded_len=" << generic_bytes.size() << " grpc_frame_len=" << envelope.frame.size() << "\n";
    std::cout << "encoded_hex=" << HexPreview(generic_bytes, 64) << "\n";
    return 0;
}
