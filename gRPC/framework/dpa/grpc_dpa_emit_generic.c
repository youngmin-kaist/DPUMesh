#include "grpc_dpa_emit_iface.h"

#include <string.h>

namespace dmesh::grpc_codegen {

static inline uint32_t MakeTag(uint32_t field_no, WireType wire_type)
{
    return (field_no << 3U) | static_cast<uint32_t>(wire_type);
}

static void PutVarint(uint64_t v, ByteBuffer *out)
{
    while (v >= 0x80U) {
        out->push_back(static_cast<uint8_t>((v & 0x7fU) | 0x80U));
        v >>= 7U;
    }
    out->push_back(static_cast<uint8_t>(v));
}

static Status EmitLengthDelimitedField(uint32_t field_no, const HybridFieldRef &hybrid, ByteBuffer *out)
{
    ByteSpan span{};

    if (std::holds_alternative<InlineCopyRef>(hybrid)) {
        const InlineCopyRef &copy = std::get<InlineCopyRef>(hybrid);
        span.data = copy.bytes.data();
        span.len = static_cast<uint32_t>(copy.bytes.size());
    } else {
        const ExternalZeroCopyRef &zref = std::get<ExternalZeroCopyRef>(hybrid);
        span = zref.span;
    }

    PutVarint(MakeTag(field_no, WireType::kLengthDelimited), out);
    PutVarint(span.len, out);
    out->insert(out->end(), span.data, span.data + span.len);
    return Status::Ok();
}

static Status EmitPlanInternal(const ExecPlan &plan, ByteBuffer *out)
{
    for (const EmitOp &op : plan.ops) {
        switch (op.kind) {
        case EmitOp::Kind::kVarint:
            PutVarint(MakeTag(op.field_no, WireType::kVarint), out);
            PutVarint(op.scalar.u64, out);
            break;
        case EmitOp::Kind::kLengthDelimited:
            if (!EmitLengthDelimitedField(op.field_no, op.hybrid, out).ok())
                return Status::Internal("length-delimited emit failed");
            break;
        case EmitOp::Kind::kPackedVarints: {
            ByteBuffer tmp;
            for (uint64_t v : op.packed_values)
                PutVarint(v, &tmp);
            PutVarint(MakeTag(op.field_no, WireType::kLengthDelimited), out);
            PutVarint(tmp.size(), out);
            out->insert(out->end(), tmp.begin(), tmp.end());
            break;
        }
        case EmitOp::Kind::kStartSubMessage: {
            if (op.nested_plan_index >= plan.nested_plans.size())
                return Status::Invalid("nested plan index out of range");
            ByteBuffer tmp;
            Status st = EmitPlanInternal(plan.nested_plans[op.nested_plan_index], &tmp);
            if (!st.ok())
                return st;
            PutVarint(MakeTag(op.field_no, WireType::kLengthDelimited), out);
            PutVarint(tmp.size(), out);
            out->insert(out->end(), tmp.begin(), tmp.end());
            break;
        }
        case EmitOp::Kind::kEndSubMessage:
            break;
        }
    }
    return Status::Ok();
}

Status EmitExecPlanGeneric(const ExecPlan &plan, ByteBuffer *out)
{
    if (out == nullptr)
        return Status::Invalid("null output buffer");
    out->clear();
    return EmitPlanInternal(plan, out);
}

}  // namespace dmesh::grpc_codegen
