#include "demo_request_lowering.h"

namespace dmesh::grpc_codegen::demo_generated {
namespace {

void CountHybrid(const HybridFieldRef &ref, LoweringSummary *summary)
{
    if (summary == nullptr)
        return;
    if (std::holds_alternative<ExternalZeroCopyRef>(ref))
        summary->zero_copy_fields++;
    else
        summary->copied_fields++;
}

Status LowerInner(const InnerView &inner,
                  const LoweringContext &ctx,
                  ExecPlan *plan,
                  LoweringSummary *summary)
{
    if (plan == nullptr)
        return Status::Invalid("null inner plan");

    plan->message_desc = GetInnerDesc();
    plan->backend = BackendKind::kGenericPlan;
    plan->schema_hash = GetInnerDesc()->schema_hash;
    plan->schema_version = GetInnerDesc()->schema_version;
    plan->bounds = ctx.bounds;

    if (inner.has_note) {
        BufferCandidate candidate = inner.note.AsCandidate();
        SelectiveOffloadDecision decision = ctx.policy->DecideBytesLike(candidate, ctx.selective_ctx);
        EmitOp op{};
        op.kind = EmitOp::Kind::kLengthDelimited;
        op.field_no = 1;
        op.field_kind = FieldKind::kString;
        op.hybrid = MakeHybridFieldRef(candidate, decision);
        CountHybrid(op.hybrid, summary);
        plan->ops.push_back(std::move(op));
    }

    EmitOp ts_op{};
    ts_op.kind = EmitOp::Kind::kVarint;
    ts_op.field_no = 2;
    ts_op.field_kind = FieldKind::kUint64;
    ts_op.scalar.u64 = inner.ts;
    plan->ops.push_back(std::move(ts_op));
    return Status::Ok();
}

Status LowerAttrEntryValue(const AttrEntryValueView &value,
                           const LoweringContext &ctx,
                           ExecPlan *plan,
                           LoweringSummary *summary)
{
    if (plan == nullptr)
        return Status::Invalid("null attr value plan");

    plan->message_desc = GetAttrEntryValueDesc();
    plan->backend = BackendKind::kGenericPlan;
    plan->schema_hash = GetAttrEntryValueDesc()->schema_hash;
    plan->schema_version = GetAttrEntryValueDesc()->schema_version;
    plan->bounds = ctx.bounds;

    switch (value.kind) {
    case AttrEntryValueCase::kNum: {
        EmitOp op{};
        op.kind = EmitOp::Kind::kVarint;
        op.field_no = 1;
        op.field_kind = FieldKind::kUint64;
        op.scalar.u64 = value.num;
        plan->ops.push_back(std::move(op));
        return Status::Ok();
    }
    case AttrEntryValueCase::kText: {
        BufferCandidate candidate = value.text.AsCandidate();
        SelectiveOffloadDecision decision = ctx.policy->DecideBytesLike(candidate, ctx.selective_ctx);
        EmitOp op{};
        op.kind = EmitOp::Kind::kLengthDelimited;
        op.field_no = 2;
        op.field_kind = FieldKind::kString;
        op.hybrid = MakeHybridFieldRef(candidate, decision);
        CountHybrid(op.hybrid, summary);
        plan->ops.push_back(std::move(op));
        return Status::Ok();
    }
    case AttrEntryValueCase::kNone:
        return Status::Ok();
    }

    return Status::Unsupported("unsupported AttrEntryValue oneof case");
}

Status LowerAttrEntry(const std::pair<PayloadBufferInput, AttrEntryValueView> &attr,
                      const LoweringContext &ctx,
                      ExecPlan *plan,
                      LoweringSummary *summary)
{
    if (plan == nullptr)
        return Status::Invalid("null attr entry plan");

    plan->message_desc = GetAttrEntryDesc();
    plan->backend = BackendKind::kGenericPlan;
    plan->schema_hash = GetAttrEntryDesc()->schema_hash;
    plan->schema_version = GetAttrEntryDesc()->schema_version;
    plan->bounds = ctx.bounds;

    BufferCandidate key_candidate = attr.first.AsCandidate();
    SelectiveOffloadDecision key_decision = ctx.policy->DecideBytesLike(key_candidate, ctx.selective_ctx);
    EmitOp key_op{};
    key_op.kind = EmitOp::Kind::kLengthDelimited;
    key_op.field_no = 1;
    key_op.field_kind = FieldKind::kString;
    key_op.hybrid = MakeHybridFieldRef(key_candidate, key_decision);
    CountHybrid(key_op.hybrid, summary);
    plan->ops.push_back(std::move(key_op));

    ExecPlan value_plan{};
    Status st = LowerAttrEntryValue(attr.second, ctx, &value_plan, summary);
    if (!st.ok())
        return st;
    plan->nested_plans.push_back(std::move(value_plan));

    EmitOp value_op{};
    value_op.kind = EmitOp::Kind::kStartSubMessage;
    value_op.field_no = 2;
    value_op.field_kind = FieldKind::kMessage;
    value_op.nested_plan_index = static_cast<uint32_t>(plan->nested_plans.size() - 1U);
    value_op.submessage_desc = GetAttrEntryValueDesc();
    plan->ops.push_back(std::move(value_op));
    return Status::Ok();
}

}  // namespace

Status BuildRequestExecPlanGeneric(const RequestView &request,
                                   const LoweringContext &ctx,
                                   ExecPlan *plan,
                                   LoweringSummary *summary)
{
    ExecPlan tmp{};
    Status st;

    if (plan == nullptr || ctx.policy == nullptr)
        return Status::Invalid("null plan or policy");
    if (request.attrs.size() > ctx.bounds.max_map_entries)
        return Status::Resource("map entry limit exceeded");
    if (request.scores.size() > ctx.bounds.max_repeated_count)
        return Status::Resource("repeated score limit exceeded");

    tmp.message_desc = GetRequestDesc();
    tmp.backend = BackendKind::kGenericPlan;
    tmp.schema_hash = GetRequestDesc()->schema_hash;
    tmp.schema_version = GetRequestDesc()->schema_version;
    tmp.bounds = ctx.bounds;

    EmitOp id_op{};
    id_op.kind = EmitOp::Kind::kVarint;
    id_op.field_no = 1;
    id_op.field_kind = FieldKind::kUint64;
    id_op.scalar.u64 = request.id;
    tmp.ops.push_back(std::move(id_op));

    if (request.has_name) {
        BufferCandidate candidate = request.name.AsCandidate();
        SelectiveOffloadDecision decision = ctx.policy->DecideBytesLike(candidate, ctx.selective_ctx);
        EmitOp op{};
        op.kind = EmitOp::Kind::kLengthDelimited;
        op.field_no = 2;
        op.field_kind = FieldKind::kString;
        op.hybrid = MakeHybridFieldRef(candidate, decision);
        CountHybrid(op.hybrid, summary);
        tmp.ops.push_back(std::move(op));
    }

    {
        ExecPlan inner_plan{};
        st = LowerInner(request.inner, ctx, &inner_plan, summary);
        if (!st.ok())
            return st;
        tmp.nested_plans.push_back(std::move(inner_plan));
        EmitOp op{};
        op.kind = EmitOp::Kind::kStartSubMessage;
        op.field_no = 3;
        op.field_kind = FieldKind::kMessage;
        op.nested_plan_index = static_cast<uint32_t>(tmp.nested_plans.size() - 1U);
        op.submessage_desc = GetInnerDesc();
        tmp.ops.push_back(std::move(op));
    }

    for (const auto &attr : request.attrs) {
        ExecPlan attr_plan{};
        st = LowerAttrEntry(attr, ctx, &attr_plan, summary);
        if (!st.ok())
            return st;
        tmp.nested_plans.push_back(std::move(attr_plan));
        EmitOp op{};
        op.kind = EmitOp::Kind::kStartSubMessage;
        op.field_no = 4;
        op.field_kind = FieldKind::kMapEntry;
        op.nested_plan_index = static_cast<uint32_t>(tmp.nested_plans.size() - 1U);
        op.submessage_desc = GetAttrEntryDesc();
        tmp.ops.push_back(std::move(op));
    }

    switch (request.payload_case) {
    case RequestPayloadCase::kRaw: {
        BufferCandidate candidate = request.raw.AsCandidate();
        SelectiveOffloadDecision decision = ctx.policy->DecideBytesLike(candidate, ctx.selective_ctx);
        EmitOp op{};
        op.kind = EmitOp::Kind::kLengthDelimited;
        op.field_no = 5;
        op.field_kind = FieldKind::kBytes;
        op.hybrid = MakeHybridFieldRef(candidate, decision);
        CountHybrid(op.hybrid, summary);
        tmp.ops.push_back(std::move(op));
        break;
    }
    case RequestPayloadCase::kNested: {
        ExecPlan nested_plan{};
        st = LowerInner(request.nested, ctx, &nested_plan, summary);
        if (!st.ok())
            return st;
        tmp.nested_plans.push_back(std::move(nested_plan));
        EmitOp op{};
        op.kind = EmitOp::Kind::kStartSubMessage;
        op.field_no = 6;
        op.field_kind = FieldKind::kMessage;
        op.nested_plan_index = static_cast<uint32_t>(tmp.nested_plans.size() - 1U);
        op.submessage_desc = GetInnerDesc();
        tmp.ops.push_back(std::move(op));
        break;
    }
    case RequestPayloadCase::kNone:
        break;
    }

    if (!request.scores.empty()) {
        EmitOp scores_op{};
        scores_op.kind = EmitOp::Kind::kPackedVarints;
        scores_op.field_no = 7;
        scores_op.field_kind = FieldKind::kPackedUint32;
        scores_op.packed_values.assign(request.scores.begin(), request.scores.end());
        tmp.ops.push_back(std::move(scores_op));
    }

    tmp.zero_copy_field_count = summary ? summary->zero_copy_fields : 0;
    tmp.copied_field_count = summary ? summary->copied_fields : 0;
    *plan = std::move(tmp);
    return Status::Ok();
}

}  // namespace dmesh::grpc_codegen::demo_generated
