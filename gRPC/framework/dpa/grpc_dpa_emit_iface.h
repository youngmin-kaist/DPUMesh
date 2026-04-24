#ifndef DMESH_GRPC_DPA_EMIT_IFACE_H
#define DMESH_GRPC_DPA_EMIT_IFACE_H

#include "../ir/grpc_exec_plan.h"

namespace dmesh::grpc_codegen {

class GenericDpaEmitter {
public:
    virtual ~GenericDpaEmitter() = default;
    virtual Status Emit(const ExecPlan &plan, ByteBuffer *out) const = 0;
};

class SpecializedDpaEmitter {
public:
    virtual ~SpecializedDpaEmitter() = default;
    virtual Status EmitSpecialized(ByteBuffer *out) const = 0;
};

Status EmitExecPlanGeneric(const ExecPlan &plan, ByteBuffer *out);

}  // namespace dmesh::grpc_codegen

#endif
