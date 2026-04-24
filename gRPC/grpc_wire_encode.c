#include "grpc_wire_encode.h"

#include <stdint.h>

static inline uint8_t ld_u8(const uint8_t *p)
{
    return *p;
}

static inline uint32_t ld_u32(const uint32_t *p)
{
    return *p;
}

static inline uint64_t ld_u64(const uint64_t *p)
{
    return *p;
}

static inline uint8_t *put_varint_checked(uint8_t *out, const uint8_t *end, uint64_t v)
{
    while (v >= 0x80U) {
        if (out >= end)
            return NULL;
        *out++ = (uint8_t)((v & 0x7fU) | 0x80U);
        v >>= 7U;
    }
    if (out >= end)
        return NULL;
    *out++ = (uint8_t)v;
    return out;
}

static inline uint32_t measure_varint_u32(uint32_t v)
{
    uint32_t n = 1U;

    while (v >= 0x80U) {
        v >>= 7U;
        ++n;
    }
    return n;
}

static inline uint32_t measure_varint_u64(uint64_t v)
{
    uint32_t n = 1U;

    while (v >= 0x80U) {
        v >>= 7U;
        ++n;
    }
    return n;
}

static inline uint64_t make_tag(uint32_t field_no, uint8_t wire_type)
{
    return (((uint64_t)field_no) << 3) | wire_type;
}

static inline void put_grpc_header(uint8_t *out, uint32_t payload_len)
{
    out[0] = 0;
    out[1] = (uint8_t)((payload_len >> 24) & 0xffU);
    out[2] = (uint8_t)((payload_len >> 16) & 0xffU);
    out[3] = (uint8_t)((payload_len >> 8) & 0xffU);
    out[4] = (uint8_t)(payload_len & 0xffU);
}

static uint8_t *backfill_len_varint_compact(uint8_t *len_pos,
                                            uint8_t *payload_start,
                                            uint32_t payload_len)
{
    uint8_t tmp[5];
    uint8_t *len_end;
    uint32_t len_sz;
    uint32_t shift;
    uint32_t i;

    if (payload_len > PROTO_MAX_LEN_DELIMITED)
        return NULL;

    len_end = put_varint_checked(tmp, tmp + sizeof(tmp), payload_len);
    if (len_end == NULL)
        return NULL;

    len_sz = (uint32_t)(len_end - tmp);
    shift = 5U - len_sz;

    if (shift > 0U) {
        for (i = 0; i < payload_len; ++i)
            len_pos[len_sz + i] = payload_start[i];
    }

    for (i = 0; i < len_sz; ++i)
        len_pos[i] = tmp[i];

    return len_pos + len_sz + payload_len;
}

static const ProtoMessageDesc *find_desc(const ProtoDescBlob *blob, uint32_t desc_id)
{
    uint32_t i;

    for (i = 0; i < blob->msg_count; ++i) {
        if (blob->msgs[i].desc_id == desc_id)
            return &blob->msgs[i];
    }

    return NULL;
}

static const ProtoFieldDesc *desc_field_at(const ProtoDescBlob *blob,
                                           const ProtoMessageDesc *desc,
                                           uint32_t idx)
{
    uint32_t field_idx = desc->field_begin + idx;

    if (field_idx >= blob->field_count)
        return NULL;

    return &blob->fields[field_idx];
}

static uint8_t *emit_message_generic(const ProtoDescBlob *blob,
                                     const ProtoMessageDesc *desc,
                                     const uint8_t *base,
                                     uint8_t *out,
                                     const uint8_t *end);

static uint8_t *emit_hello_request_specialized(const uint8_t *base,
                                               uint8_t *out,
                                               const uint8_t *end)
{
    const HelloRequestFlat *flat = (const HelloRequestFlat *)base;
    const DpaStringRef *name = &flat->name;
    const DpaU32ArrayRef *scores = &flat->scores;
    const uint8_t *name_bytes;
    const uint32_t *score_ptr;
    uint8_t *len_pos;
    uint8_t *payload_start;
    uint32_t i;

    if (flat->id != 0U) {
        out = put_varint_checked(out, end, make_tag(1U, WIRE_VARINT));
        if (out == NULL)
            return NULL;
        out = put_varint_checked(out, end, flat->id);
        if (out == NULL)
            return NULL;
    }

    if (name->len != 0U) {
        if (name->len > PROTO_MAX_LEN_DELIMITED)
            return NULL;
        name_bytes = base + name->offset;
        out = put_varint_checked(out, end, make_tag(2U, WIRE_LEN));
        if (out == NULL)
            return NULL;
        out = put_varint_checked(out, end, name->len);
        if (out == NULL)
            return NULL;
        for (i = 0; i < name->len; ++i) {
            if (out >= end)
                return NULL;
            *out++ = ld_u8(name_bytes + i);
        }
    }

    if (scores->count != 0U) {
        score_ptr = (const uint32_t *)(base + scores->offset);
        out = put_varint_checked(out, end, make_tag(3U, WIRE_LEN));
        if (out == NULL || out + 5U > end)
            return NULL;

        len_pos = out;
        out += 5U;
        payload_start = out;

        for (i = 0; i < scores->count; ++i) {
            out = put_varint_checked(out, end, ld_u32(score_ptr + i));
            if (out == NULL)
                return NULL;
        }

        return backfill_len_varint_compact(len_pos, payload_start, (uint32_t)(out - payload_start));
    }

    return out;
}

static uint8_t *emit_field_generic(const ProtoDescBlob *blob,
                                   const ProtoFieldDesc *f,
                                   const uint8_t *base,
                                   uint8_t *out,
                                   const uint8_t *end)
{
    switch (f->kind) {
    case FK_U32: {
        uint32_t v = ld_u32((const uint32_t *)(base + f->offset));
        if (v == 0U)
            return out;
        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_VARINT));
        if (out == NULL)
            return NULL;
        return put_varint_checked(out, end, v);
    }
    case FK_U64: {
        uint64_t v = ld_u64((const uint64_t *)(base + f->offset));
        if (v == 0U)
            return out;
        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_VARINT));
        if (out == NULL)
            return NULL;
        return put_varint_checked(out, end, v);
    }
    case FK_BOOL: {
        uint8_t v = ld_u8((const uint8_t *)(base + f->offset));
        if (v == 0U)
            return out;
        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_VARINT));
        if (out == NULL || out >= end)
            return NULL;
        *out++ = 1U;
        return out;
    }
    case FK_STRING:
    case FK_BYTES: {
        const DpaStringRef *s = (const DpaStringRef *)(base + f->offset);
        const uint8_t *p;
        uint32_t i;

        if (s->len == 0U)
            return out;
        if (s->len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        p = base + s->offset;
        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_LEN));
        if (out == NULL)
            return NULL;
        out = put_varint_checked(out, end, s->len);
        if (out == NULL)
            return NULL;
        for (i = 0; i < s->len; ++i) {
            if (out >= end)
                return NULL;
            *out++ = ld_u8(p + i);
        }
        return out;
    }
    case FK_REPEATED_U32_PACKED: {
        const DpaU32ArrayRef *arr = (const DpaU32ArrayRef *)(base + f->offset);
        const uint32_t *p = (const uint32_t *)(base + arr->offset);
        uint8_t *len_pos;
        uint8_t *payload_start;
        uint32_t payload_len = 0U;
        uint32_t i;

        if (arr->count == 0U)
            return out;

        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_LEN));
        if (out == NULL || out + 5U > end)
            return NULL;

        len_pos = out;
        out += 5U;
        payload_start = out;

        for (i = 0; i < arr->count; ++i) {
            uint32_t v = ld_u32(p + i);

            payload_len += measure_varint_u32(v);
            if (payload_len > PROTO_MAX_LEN_DELIMITED)
                return NULL;

            out = put_varint_checked(out, end, v);
            if (out == NULL)
                return NULL;
        }

        return backfill_len_varint_compact(len_pos, payload_start, payload_len);
    }
    case FK_REPEATED_U64_PACKED: {
        const DpaU64ArrayRef *arr = (const DpaU64ArrayRef *)(base + f->offset);
        const uint64_t *p = (const uint64_t *)(base + arr->offset);
        uint8_t *len_pos;
        uint8_t *payload_start;
        uint32_t payload_len = 0U;
        uint32_t i;

        if (arr->count == 0U)
            return out;

        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_LEN));
        if (out == NULL || out + 5U > end)
            return NULL;

        len_pos = out;
        out += 5U;
        payload_start = out;

        for (i = 0; i < arr->count; ++i) {
            uint64_t v = ld_u64(p + i);

            payload_len += measure_varint_u64(v);
            if (payload_len > PROTO_MAX_LEN_DELIMITED)
                return NULL;

            out = put_varint_checked(out, end, v);
            if (out == NULL)
                return NULL;
        }

        return backfill_len_varint_compact(len_pos, payload_start, payload_len);
    }
    case FK_MESSAGE: {
        uint32_t child_off = ld_u32((const uint32_t *)(base + f->offset));
        const ProtoMessageDesc *child_desc;
        const uint8_t *child;
        uint8_t *len_pos;
        uint8_t *payload_start;
        uint8_t *child_end;
        uint32_t payload_len;

        if (child_off == 0U)
            return out;

        child_desc = find_desc(blob, f->child_desc_id);
        if (child_desc == NULL)
            return NULL;

        child = base + child_off;
        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_LEN));
        if (out == NULL || out + 5U > end)
            return NULL;

        len_pos = out;
        out += 5U;
        payload_start = out;
        child_end = emit_message_generic(blob, child_desc, child, out, end);
        if (child_end == NULL)
            return NULL;

        payload_len = (uint32_t)(child_end - payload_start);
        if (payload_len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        return backfill_len_varint_compact(len_pos, payload_start, payload_len);
    }
    default:
        return NULL;
    }
}

static uint8_t *emit_message_generic(const ProtoDescBlob *blob,
                                     const ProtoMessageDesc *desc,
                                     const uint8_t *base,
                                     uint8_t *out,
                                     const uint8_t *end)
{
    uint32_t i;

    for (i = 0; i < desc->field_count; ++i) {
        const ProtoFieldDesc *f = desc_field_at(blob, desc, i);

        if (f == NULL)
            return NULL;
        out = emit_field_generic(blob, f, base, out, end);
        if (out == NULL)
            return NULL;
    }

    return out;
}

static uint8_t *emit_message_dispatch(const ProtoDescBlob *blob,
                                      const ProtoMessageDesc *desc,
                                      const uint8_t *base,
                                      uint8_t *out,
                                      const uint8_t *end,
                                      GrpcWireEncodeStats *stats)
{
    if (stats != NULL)
        stats->specialized_attempts++;

    if (desc->desc_id == 1U && desc->field_count == 3U) {
        uint8_t *ret = emit_hello_request_specialized(base, out, end);

        if (ret != NULL) {
            if (stats != NULL)
                stats->specialized_hits++;
            return ret;
        }
    }

    if (stats != NULL)
        stats->generic_fallbacks++;
    return emit_message_generic(blob, desc, base, out, end);
}

void grpc_wire_encode_stats_reset(GrpcWireEncodeStats *stats)
{
    if (stats == NULL)
        return;
    stats->specialized_attempts = 0U;
    stats->specialized_hits = 0U;
    stats->generic_fallbacks = 0U;
}

int grpc_wire_serialize_one(const ProtoDescBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats)
{
    const ProtoMessageDesc *desc;
    const uint8_t *msg_base;
    uint8_t *out_base;
    uint8_t *out_end;
    const uint8_t *cap_end;
    uint32_t payload_len;

    if (blob == NULL || task == NULL || cpl == NULL)
        return -1;

    desc = find_desc(blob, task->desc_id);
    if (desc == NULL) {
        cpl->request_id = task->request_id;
        cpl->encoded_len = 0U;
        cpl->status = -1;
        return -1;
    }

    if (task->dpa_out_cap < 5U) {
        cpl->request_id = task->request_id;
        cpl->encoded_len = 0U;
        cpl->status = -2;
        return -1;
    }

    msg_base = (const uint8_t *)(uintptr_t)task->dpa_msg_addr;
    out_base = (uint8_t *)(uintptr_t)task->dpa_out_addr;
    cap_end = out_base + task->dpa_out_cap;

    out_end = emit_message_dispatch(blob, desc, msg_base, out_base + 5U, cap_end, stats);
    if (out_end == NULL) {
        cpl->request_id = task->request_id;
        cpl->encoded_len = 0U;
        cpl->status = -2;
        return -1;
    }

    payload_len = (uint32_t)(out_end - (out_base + 5U));
    if (payload_len > PROTO_MAX_LEN_DELIMITED) {
        cpl->request_id = task->request_id;
        cpl->encoded_len = 0U;
        cpl->status = -3;
        return -1;
    }

    put_grpc_header(out_base, payload_len);
    cpl->request_id = task->request_id;
    cpl->encoded_len = (uint32_t)(out_end - out_base);
    cpl->status = 0;
    return 0;
}
