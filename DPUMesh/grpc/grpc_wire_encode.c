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
    uint8_t *len_end;
    uint32_t len_sz;
    uint32_t shift;
    uint32_t i;

    if (payload_len > PROTO_MAX_LEN_DELIMITED)
        return NULL;

    len_end = put_varint_checked(len_pos, payload_start, payload_len);
    if (len_end == NULL)
        return NULL;

    len_sz = (uint32_t)(len_end - len_pos);
    shift = 5U - len_sz;

    // shift payload to the right if varint length prefix is shorter than 5 bytes
    if (shift > 0U) {
        for (i = 0; i < payload_len; ++i)
            len_end[i] = payload_start[i];
    }

    return len_end + payload_len;
}

static const ProtoMessageSchema *find_desc(const ProtoSchemaBlob *blob, uint32_t schema_id)
{
    uint32_t i;

    for (i = 0; i < blob->msg_count; ++i) {
        if (blob->msgs[i].schema_id == schema_id)
            return &blob->msgs[i];
    }

    return NULL;
}

static const ProtoFieldSchema *desc_field_at(const ProtoSchemaBlob *blob,
                                           const ProtoMessageSchema *desc,
                                           uint32_t idx)
{
    uint32_t field_idx = desc->field_begin + idx;

    if (field_idx >= blob->field_count)
        return NULL;

    return &blob->fields[field_idx];
}

static uint8_t *emit_message_generic(const ProtoSchemaBlob *blob,
                                     const ProtoMessageSchema *desc,
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
    const uint8_t *name_ptr;
    const uint32_t *score_ptr;
    uint8_t *len_pos;
    uint8_t *payload_start;
    uint32_t i;

    // id field
    out = put_varint_checked(out, end, make_tag(1U, WIRE_VARINT));
    if (out == NULL)
        return NULL;
    out = put_varint_checked(out, end, flat->id);
    if (out == NULL)
        return NULL;

    // name field
    if (name->len != 0U) {
        if (name->len > PROTO_MAX_LEN_DELIMITED)
            return NULL;
        out = put_varint_checked(out, end, make_tag(2U, WIRE_LEN));
        if (out == NULL)
            return NULL;
        out = put_varint_checked(out, end, name->len);
        if (out == NULL)
            return NULL;

        name_ptr = base + name->offset;
        for (i = 0; i < name->len; ++i) {
            if (out >= end)
                return NULL;
            *(out++) = *(name_ptr++);
        }
    }

    // scores field
    if (scores->count != 0U) {
        out = put_varint_checked(out, end, make_tag(3U, WIRE_LEN));
        if (out == NULL || out + 5U > end)
        return NULL;
        
        len_pos = out;
        out += 5U;
        payload_start = out;
        
        score_ptr = (const uint32_t *)(base + scores->offset);
        for (i = 0; i < scores->count; ++i) {
            out = put_varint_checked(out, end, ld_u32(score_ptr++));
            if (out == NULL)
                return NULL;
        }

        return backfill_len_varint_compact(len_pos, payload_start, (uint32_t)(out - payload_start));
    }

    return out;
}

static uint8_t *emit_field_generic(const ProtoSchemaBlob *blob,
                                   const ProtoFieldSchema *f,
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
    case FK_STRING: // ignore UTF-8/ASCII validation and treat as bytes
    case FK_BYTES: {
        const DpaStringRef *s = (const DpaStringRef *)(base + f->offset);
        const uint8_t *p;
        uint32_t i;

        if (s->len == 0U)
            return out;
        if (s->len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_LEN));
        if (out == NULL)
            return NULL;
        out = put_varint_checked(out, end, s->len);
        if (out == NULL)
            return NULL;

        p = base + s->offset;
        for (i = 0; i < s->len; ++i) {
            if (out >= end)
                return NULL;
            *out++ = ld_u8(p + i);
        }
        return out;
    }
    case FK_REPEATED_U32_PACKED: {
        const DpaU32ArrayRef *arr = (const DpaU32ArrayRef *)(base + f->offset);
        const uint32_t *p;
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

        p = (const uint32_t *)(base + arr->offset);
        for (i = 0; i < arr->count; ++i) {
            uint32_t v = ld_u32(p + i);
            uint8_t *next_out;
            
            next_out = put_varint_checked(out, end, v);
            if (next_out == NULL)
                return NULL;

            payload_len += (uint32_t)(next_out - out);
            if (payload_len > PROTO_MAX_LEN_DELIMITED)
                return NULL;
            
            out = next_out;
        }

        return backfill_len_varint_compact(len_pos, payload_start, payload_len);
    }
    case FK_REPEATED_U64_PACKED: {
        const DpaU64ArrayRef *arr = (const DpaU64ArrayRef *)(base + f->offset);
        const uint64_t *p;
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

        p = (const uint64_t *)(base + arr->offset);
        for (i = 0; i < arr->count; ++i) {
            uint64_t v = ld_u64(p + i);
            uint8_t *next_out;

            next_out = put_varint_checked(out, end, v);
            if (next_out == NULL)
                return NULL;

            payload_len += (uint32_t)(next_out - out);
            if (payload_len > PROTO_MAX_LEN_DELIMITED)
                return NULL;

            out = next_out;
        }

        return backfill_len_varint_compact(len_pos, payload_start, payload_len);
    }
    case FK_MESSAGE: {
        uint32_t child_off = ld_u32((const uint32_t *)(base + f->offset));
        const ProtoMessageSchema *child_desc;
        const uint8_t *child;
        uint8_t *len_pos;
        uint8_t *payload_start;
        uint8_t *child_end;
        uint32_t payload_len;

        if (child_off == 0U)
            return out;
            
        child_desc = find_desc(blob, f->child_schema_id);
        if (child_desc == NULL)
            return NULL;
            
        out = put_varint_checked(out, end, make_tag(f->field_no, WIRE_LEN));
        if (out == NULL || out + 5U > end)
            return NULL;
            
        child = base + child_off;
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

static uint8_t *emit_message_generic(const ProtoSchemaBlob *blob,
                                     const ProtoMessageSchema *desc,
                                     const uint8_t *base,
                                     uint8_t *out,
                                     const uint8_t *end)
{
    uint32_t i;

    for (i = 0; i < desc->field_count; ++i) {
        const ProtoFieldSchema *f = desc_field_at(blob, desc, i);
        if (f == NULL)
            return NULL;

        out = emit_field_generic(blob, f, base, out, end);
        if (out == NULL)
            return NULL;
    }

    return out;
}

static uint8_t *emit_message_dispatch(const ProtoSchemaBlob *blob,
                                      const ProtoMessageSchema *desc,
                                      const uint8_t *base,
                                      uint8_t *out,
                                      const uint8_t *end,
                                      GrpcWireEncodeStats *stats)
{
    if (stats != NULL)
        stats->specialized_attempts++;

    if (desc->schema_id == DMESH_GRPC_SCHEMA_HELLO_REQUEST) {
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

int grpc_wire_serialize_one(const ProtoSchemaBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats)
{
    const ProtoMessageSchema *desc;
    const uint8_t *flat_base;
    uint8_t *out_base;
    uint8_t *out_end;
    const uint8_t *cap_end;
    uint32_t payload_len;

    if (blob == NULL || task == NULL || cpl == NULL)
        return -1;
    cpl->request_id = task->request_id;

    desc = find_desc(blob, task->schema_id);
    if (desc == NULL) {
        cpl->status = -1;
        goto error;
    }

    if (task->dpa_out_cap < 5U) {
        cpl->status = -2;
        goto error;
    }

    flat_base = (const uint8_t *)(uintptr_t)task->dpa_msg_addr;
    out_base = (uint8_t *)(uintptr_t)task->dpa_out_addr;
    cap_end = out_base + task->dpa_out_cap;

    out_end = emit_message_dispatch(blob, desc, flat_base, out_base + 5U, cap_end, stats);
    if (out_end == NULL) {
        cpl->status = -2;
        goto error;
    }

    payload_len = (uint32_t)(out_end - (out_base + 5U));
    if (payload_len > PROTO_MAX_LEN_DELIMITED) {
        cpl->status = -3;
        goto error;
    }

    put_grpc_header(out_base, payload_len);
    cpl->encoded_len = (uint32_t)(out_end - out_base);
    cpl->status = 0;
    return 0;

error:
    cpl->encoded_len = 0U;
    return -1;
}


/* Reverse field number order encoding implementation */
/*
 * Write a protobuf varint immediately before cur and return its new start.
 *
 * The varint byte sequence itself is still the normal protobuf low-to-high
 * wire encoding.  Only the allocation direction of the output cursor is
 * reversed.  Example: value 300 becomes bytes {0xac, 0x02}; if cur points to
 * offset 100, the function stores 0xac at 98 and 0x02 at 99.
 */
static uint8_t *put_varint_reverse_checked(uint8_t *cur,
                                           const uint8_t *begin,
                                           uint64_t v)
{
    uint8_t tmp[10];
    uint32_t n = 0U;
    uint32_t i;

    do {
        uint8_t b = (uint8_t)(v & 0x7fU);
        v >>= 7U;
        if (v != 0U)
            b = (uint8_t)(b | 0x80U);
        tmp[n++] = b;
    } while (v != 0U);

    if (cur < begin || (uint32_t)(cur - begin) < n)
        return NULL;

    for (i = n; i > 0U; --i)
        *--cur = tmp[i - 1U];

    return cur;
}

static uint8_t *put_bytes_reverse_checked(uint8_t *cur,
                                          const uint8_t *begin,
                                          const uint8_t *src,
                                          uint32_t len)
{
    uint32_t i;

    if (cur < begin || (uint32_t)(cur - begin) < len)
        return NULL;

    cur -= len;
    for (i = 0U; i < len; ++i)
        cur[i] = ld_u8(src + i);

    return cur;
}

static uint8_t *emit_message_generic_reverse(const ProtoSchemaBlob *blob,
                                             const ProtoMessageSchema *desc,
                                             const uint8_t *base,
                                             uint8_t *cur,
                                             const uint8_t *begin);

static uint8_t *emit_hello_request_specialized_reverse(const uint8_t *base,
                                                       uint8_t *cur,
                                                       const uint8_t *begin)
{
    const HelloRequestFlat *flat = (const HelloRequestFlat *)base;
    const DpaStringRef *name = &flat->name;
    const DpaU32ArrayRef *scores = &flat->scores;
    const uint8_t *name_ptr;
    const uint32_t *score_ptr;
    uint8_t *payload_end;
    uint32_t payload_len;
    uint32_t i;

    /* field 3: repeated uint32 scores = 3 [packed=true] */
    if (scores->count != 0U) {
        payload_end = cur;
        score_ptr = (const uint32_t *)(base + scores->offset);

        for (i = scores->count; i > 0U; --i) {
            cur = put_varint_reverse_checked(cur, begin, ld_u32(score_ptr + i - 1U));
            if (cur == NULL)
                return NULL;
        }

        payload_len = (uint32_t)(payload_end - cur);
        if (payload_len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        cur = put_varint_reverse_checked(cur, begin, payload_len);
        if (cur == NULL)
            return NULL;
        cur = put_varint_reverse_checked(cur, begin, make_tag(3U, WIRE_LEN));
        if (cur == NULL)
            return NULL;
    }

    /* field 2: string/bytes name = 2 */
    if (name->len != 0U) {
        if (name->len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        name_ptr = base + name->offset;
        cur = put_bytes_reverse_checked(cur, begin, name_ptr, name->len);
        if (cur == NULL)
            return NULL;
        cur = put_varint_reverse_checked(cur, begin, name->len);
        if (cur == NULL)
            return NULL;
        cur = put_varint_reverse_checked(cur, begin, make_tag(2U, WIRE_LEN));
        if (cur == NULL)
            return NULL;
    }

    /* field 1: uint64 id = 1.  Proto3/default-value semantics: omit zero. */
    if (flat->id != 0U) {
        cur = put_varint_reverse_checked(cur, begin, flat->id);
        if (cur == NULL)
            return NULL;
        cur = put_varint_reverse_checked(cur, begin, make_tag(1U, WIRE_VARINT));
        if (cur == NULL)
            return NULL;
    }

    return cur;
}

static uint8_t *emit_field_generic_reverse(const ProtoSchemaBlob *blob,
                                           const ProtoFieldSchema *f,
                                           const uint8_t *base,
                                           uint8_t *cur,
                                           const uint8_t *begin)
{
    switch (f->kind) {
    case FK_U32: {
        uint32_t v = ld_u32((const uint32_t *)(base + f->offset));
        if (v == 0U)
            return cur;
        cur = put_varint_reverse_checked(cur, begin, v);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_VARINT));
    }
    case FK_U64: {
        uint64_t v = ld_u64((const uint64_t *)(base + f->offset));
        if (v == 0U)
            return cur;
        cur = put_varint_reverse_checked(cur, begin, v);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_VARINT));
    }
    case FK_BOOL: {
        uint8_t v = ld_u8((const uint8_t *)(base + f->offset));
        if (v == 0U)
            return cur;
        cur = put_varint_reverse_checked(cur, begin, 1U);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_VARINT));
    }
    case FK_STRING: /* ignore UTF-8 validation and treat as bytes */
    case FK_BYTES: {
        const DpaStringRef *s = (const DpaStringRef *)(base + f->offset);
        const uint8_t *p;

        if (s->len == 0U)
            return cur;
        if (s->len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        p = base + s->offset;
        cur = put_bytes_reverse_checked(cur, begin, p, s->len);
        if (cur == NULL)
            return NULL;
        cur = put_varint_reverse_checked(cur, begin, s->len);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_LEN));
    }
    case FK_REPEATED_U32_PACKED: {
        const DpaU32ArrayRef *arr = (const DpaU32ArrayRef *)(base + f->offset);
        const uint32_t *p;
        uint8_t *payload_end;
        uint32_t payload_len;
        uint32_t i;

        if (arr->count == 0U)
            return cur;

        payload_end = cur;
        p = (const uint32_t *)(base + arr->offset);
        for (i = arr->count; i > 0U; --i) {
            cur = put_varint_reverse_checked(cur, begin, ld_u32(p + i - 1U));
            if (cur == NULL)
                return NULL;
        }

        payload_len = (uint32_t)(payload_end - cur);
        if (payload_len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        cur = put_varint_reverse_checked(cur, begin, payload_len);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_LEN));
    }
    case FK_REPEATED_U64_PACKED: {
        const DpaU64ArrayRef *arr = (const DpaU64ArrayRef *)(base + f->offset);
        const uint64_t *p;
        uint8_t *payload_end;
        uint32_t payload_len;
        uint32_t i;

        if (arr->count == 0U)
            return cur;

        payload_end = cur;
        p = (const uint64_t *)(base + arr->offset);
        for (i = arr->count; i > 0U; --i) {
            cur = put_varint_reverse_checked(cur, begin, ld_u64(p + i - 1U));
            if (cur == NULL)
                return NULL;
        }

        payload_len = (uint32_t)(payload_end - cur);
        if (payload_len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        cur = put_varint_reverse_checked(cur, begin, payload_len);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_LEN));
    }
    case FK_MESSAGE: {
        uint32_t child_off = ld_u32((const uint32_t *)(base + f->offset));
        const ProtoMessageSchema *child_desc;
        const uint8_t *child;
        uint8_t *payload_end;
        uint32_t payload_len;

        if (child_off == 0U)
            return cur;

        child_desc = find_desc(blob, f->child_schema_id);
        if (child_desc == NULL)
            return NULL;

        child = base + child_off;
        payload_end = cur;
        cur = emit_message_generic_reverse(blob, child_desc, child, cur, begin);
        if (cur == NULL)
            return NULL;

        payload_len = (uint32_t)(payload_end - cur);
        if (payload_len > PROTO_MAX_LEN_DELIMITED)
            return NULL;

        cur = put_varint_reverse_checked(cur, begin, payload_len);
        if (cur == NULL)
            return NULL;
        return put_varint_reverse_checked(cur, begin, make_tag(f->field_no, WIRE_LEN));
    }
    default:
        return NULL;
    }
}

static uint8_t *emit_message_generic_reverse(const ProtoSchemaBlob *blob,
                                             const ProtoMessageSchema *desc,
                                             const uint8_t *base,
                                             uint8_t *cur,
                                             const uint8_t *begin)
{
    uint32_t i;

    for (i = desc->field_count - 1; i >= 0; --i) {
        const ProtoFieldSchema *f = desc_field_at(blob, desc, i);
        if (f == NULL)
            return NULL;

        cur = emit_field_generic_reverse(blob, f, base, cur, begin);
        if (cur == NULL)
            return NULL;
    }

    return cur;
}

static uint8_t *emit_message_dispatch_reverse(const ProtoSchemaBlob *blob,
                                              const ProtoMessageSchema *desc,
                                              const uint8_t *base,
                                              uint8_t *cur,
                                              const uint8_t *begin,
                                              GrpcWireEncodeStats *stats)
{
    if (stats != NULL)
        stats->specialized_attempts++;

    if (desc->schema_id == DMESH_GRPC_SCHEMA_HELLO_REQUEST) {
        uint8_t *ret = emit_hello_request_specialized_reverse(base, cur, begin);

        if (ret != NULL) {
            if (stats != NULL)
                stats->specialized_hits++;
            return ret;
        }
    }

    if (stats != NULL)
        stats->generic_fallbacks++;
    return emit_message_generic_reverse(blob, desc, base, cur, begin);
}


// return offset of the encoded message in the output buffer, or -1 on error.
int grpc_wire_serialize_one_reverse(const ProtoSchemaBlob *blob,
                            const ProtoTask *task,
                            ProtoCompletion *cpl,
                            GrpcWireEncodeStats *stats)
{
    const ProtoMessageSchema *desc;
    const uint8_t *flat_base;
    uint8_t *out_base;
    uint8_t *out_begin;
    uint8_t *out_cap_end;
    uint8_t *payload_start;
    uint32_t payload_len;

    if (blob == NULL || task == NULL || cpl == NULL)
        return -1;
    cpl->request_id = task->request_id;

    desc = find_desc(blob, task->schema_id);
    if (desc == NULL) {
        cpl->status = -1;
        goto error;
    }

    if (task->dpa_out_cap < 5U) {
        cpl->status = -2;
        goto error;
    }

    flat_base = (const uint8_t *)(uintptr_t)task->dpa_msg_addr;
    out_base = (uint8_t *)(uintptr_t)task->dpa_out_addr;
    out_begin = out_base + 5U;
    out_cap_end = out_base + task->dpa_out_cap;

    /*
     * The protobuf payload is first generated from high addresses to low
     * addresses.  Length-delimited fields therefore write payload -> length ->
     * tag without reserving a temporary 5-byte length slot and without shifting
     * that field payload afterward.
     */
    payload_start = emit_message_dispatch_reverse(blob, desc, flat_base,
                                                  out_cap_end, out_begin, stats);
    if (payload_start == NULL) {
        cpl->status = -2;
        goto error;
    }

    payload_len = (uint32_t)(out_cap_end - payload_start);
    if (payload_len > PROTO_MAX_LEN_DELIMITED || payload_len > task->dpa_out_cap - 5U) {
        cpl->status = -3;
        goto error;
    }

    out_begin = payload_start - 5U;
    put_grpc_header(out_begin, payload_len);
    cpl->encoded_len = 5U + payload_len;
    cpl->status = 0;
    return out_begin - out_base;

error:
    cpl->encoded_len = 0U;
    return -1;
}
