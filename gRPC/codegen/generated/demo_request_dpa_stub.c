#include "demo_request_dpa_stub.h"

#include "demo_request_lowering.h"
#include "../../framework/dpa/grpc_dpa_emit_iface.h"

namespace dmesh::grpc_codegen::demo_generated {

Status EmitRequestSpecialized(const RequestView &request,
                              const LoweringContext &ctx,
                              ByteBuffer *out,
                              LoweringSummary *summary)
{
    ExecPlan plan{};
    Status st = BuildRequestExecPlanGeneric(request, ctx, &plan, summary);
    if (!st.ok())
        return st;
    plan.backend = BackendKind::kSpecializedCodegen;
    return EmitExecPlanGeneric(plan, out);
}

}  // namespace dmesh::grpc_codegen::demo_generated
