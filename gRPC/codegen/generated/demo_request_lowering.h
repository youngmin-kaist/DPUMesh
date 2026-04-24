#ifndef DMESH_GRPC_DEMO_REQUEST_LOWERING_H
#define DMESH_GRPC_DEMO_REQUEST_LOWERING_H

#include "demo_request_schema.h"
#include "../../framework/lowering/grpc_lowering.h"
#include "../../framework/ir/grpc_exec_plan.h"

namespace dmesh::grpc_codegen::demo_generated {

Status BuildRequestExecPlanGeneric(const RequestView &request,
                                   const LoweringContext &ctx,
                                   ExecPlan *plan,
                                   LoweringSummary *summary);

}  // namespace dmesh::grpc_codegen::demo_generated

#endif
