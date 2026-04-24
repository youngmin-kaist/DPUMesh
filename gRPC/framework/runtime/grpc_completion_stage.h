#ifndef DMESH_GRPC_COMPLETION_STAGE_H
#define DMESH_GRPC_COMPLETION_STAGE_H

#include "../ir/grpc_exec_plan.h"

namespace dmesh::grpc_codegen {

struct CompletionRecord {
    uint64_t schema_hash = 0;
    uint32_t schema_version = 0;
    size_t copied_field_count = 0;
    size_t zero_copy_field_count = 0;
    Status status = Status::Ok();
};

inline void FillCompletionFromPlan(const ExecPlan &plan, const Status &status, CompletionRecord *record)
{
    if (record == nullptr)
        return;
    record->schema_hash = plan.schema_hash;
    record->schema_version = plan.schema_version;
    record->copied_field_count = plan.copied_field_count;
    record->zero_copy_field_count = plan.zero_copy_field_count;
    record->status = status;
}

}  // namespace dmesh::grpc_codegen

#endif
