#include "grpc_codegen_verify.h"

#include <iomanip>
#include <sstream>

namespace dmesh::grpc_codegen::tests {
namespace {

uint32_t MakeTag(uint32_t field_no, WireType wire_type)
{
    return (field_no << 3U) | static_cast<uint32_t>(wire_type);
}

void PutVarint(uint64_t v, ByteBuffer *out)
{
    while (v >= 0x80U) {
        out->push_back(static_cast<uint8_t>((v & 0x7fU) | 0x80U));
        v >>= 7U;
    }
    out->push_back(static_cast<uint8_t>(v));
}

void PutBytesField(uint32_t field_no, const demo_generated::PayloadBufferInput &input, ByteBuffer *out)
{
    PutVarint(MakeTag(field_no, WireType::kLengthDelimited), out);
    PutVarint(input.bytes.size(), out);
    out->insert(out->end(), input.bytes.begin(), input.bytes.end());
}

void SerializeInner(const demo_generated::InnerView &inner, ByteBuffer *out)
{
    if (inner.has_note)
        PutBytesField(1, inner.note, out);
    PutVarint(MakeTag(2, WireType::kVarint), out);
    PutVarint(inner.ts, out);
}

void SerializeAttrEntryValue(const demo_generated::AttrEntryValueView &value, ByteBuffer *out)
{
    switch (value.kind) {
    case demo_generated::AttrEntryValueCase::kNum:
        PutVarint(MakeTag(1, WireType::kVarint), out);
        PutVarint(value.num, out);
        break;
    case demo_generated::AttrEntryValueCase::kText:
        PutBytesField(2, value.text, out);
        break;
    case demo_generated::AttrEntryValueCase::kNone:
        break;
    }
}

void SerializeAttrEntry(const std::pair<demo_generated::PayloadBufferInput, demo_generated::AttrEntryValueView> &entry,
                        ByteBuffer *out)
{
    PutBytesField(1, entry.first, out);

    ByteBuffer value_bytes;
    SerializeAttrEntryValue(entry.second, &value_bytes);
    PutVarint(MakeTag(2, WireType::kLengthDelimited), out);
    PutVarint(value_bytes.size(), out);
    out->insert(out->end(), value_bytes.begin(), value_bytes.end());
}

}  // namespace

Status SerializeWithHostReferenceFallback(const demo_generated::RequestView &request,
                                          ByteBuffer *out)
{
    ByteBuffer tmp;
    ByteBuffer nested;

    if (out == nullptr)
        return Status::Invalid("null output");

    PutVarint(MakeTag(1, WireType::kVarint), &tmp);
    PutVarint(request.id, &tmp);

    if (request.has_name)
        PutBytesField(2, request.name, &tmp);

    nested.clear();
    SerializeInner(request.inner, &nested);
    PutVarint(MakeTag(3, WireType::kLengthDelimited), &tmp);
    PutVarint(nested.size(), &tmp);
    tmp.insert(tmp.end(), nested.begin(), nested.end());

    for (const auto &attr : request.attrs) {
        nested.clear();
        SerializeAttrEntry(attr, &nested);
        PutVarint(MakeTag(4, WireType::kLengthDelimited), &tmp);
        PutVarint(nested.size(), &tmp);
        tmp.insert(tmp.end(), nested.begin(), nested.end());
    }

    switch (request.payload_case) {
    case demo_generated::RequestPayloadCase::kRaw:
        PutBytesField(5, request.raw, &tmp);
        break;
    case demo_generated::RequestPayloadCase::kNested:
        nested.clear();
        SerializeInner(request.nested, &nested);
        PutVarint(MakeTag(6, WireType::kLengthDelimited), &tmp);
        PutVarint(nested.size(), &tmp);
        tmp.insert(tmp.end(), nested.begin(), nested.end());
        break;
    case demo_generated::RequestPayloadCase::kNone:
        break;
    }

    if (!request.scores.empty()) {
        ByteBuffer packed;
        for (uint32_t v : request.scores)
            PutVarint(v, &packed);
        PutVarint(MakeTag(7, WireType::kLengthDelimited), &tmp);
        PutVarint(packed.size(), &tmp);
        tmp.insert(tmp.end(), packed.begin(), packed.end());
    }

    *out = std::move(tmp);
    return Status::Ok();
}

Status SerializeWithStandardProtobufAdapter(const demo_generated::RequestView &, ByteBuffer *)
{
    return Status::Unsupported("standard protobuf adapter unavailable: protobuf compiler/runtime headers are not installed in this repository environment");
}

Status SerializeWithPreferredReference(const demo_generated::RequestView &request,
                                       ReferenceBackendKind preferred,
                                       ByteBuffer *out,
                                       ReferenceBackendKind *used_backend)
{
    Status st;

    if (preferred == ReferenceBackendKind::kStandardProtobufAdapter) {
        st = SerializeWithStandardProtobufAdapter(request, out);
        if (st.ok()) {
            if (used_backend != nullptr)
                *used_backend = ReferenceBackendKind::kStandardProtobufAdapter;
            return st;
        }
    }

    st = SerializeWithHostReferenceFallback(request, out);
    if (st.ok() && used_backend != nullptr)
        *used_backend = ReferenceBackendKind::kFallbackHostReference;
    return st;
}

bool BuffersEqual(const ByteBuffer &a, const ByteBuffer &b)
{
    return a == b;
}

std::string HexPreview(const ByteBuffer &buf, size_t max_bytes)
{
    std::ostringstream os;
    size_t limit = buf.size() < max_bytes ? buf.size() : max_bytes;
    for (size_t i = 0; i < limit; ++i) {
        if (i != 0)
            os << ' ';
        os << std::hex << std::setw(2) << std::setfill('0') << static_cast<unsigned>(buf[i]);
    }
    if (buf.size() > limit)
        os << " ...";
    return os.str();
}

demo_generated::RequestView MakeSampleRequest()
{
    demo_generated::RequestView req{};
    req.id = 42;
    req.has_name = true;
    req.name.bytes.assign({'a', 'l', 'p', 'h', 'a'});
    req.name.dma_safe = false;
    req.name.origin = "host-stack";

    req.inner.has_note = true;
    req.inner.note.bytes.assign({'i', 'n', 'n', 'e', 'r'});
    req.inner.note.dma_safe = false;
    req.inner.ts = 999;

    demo_generated::PayloadBufferInput key1{};
    key1.bytes.assign({'k', '1'});
    demo_generated::AttrEntryValueView value1{};
    value1.kind = demo_generated::AttrEntryValueCase::kNum;
    value1.num = 7;
    req.attrs.push_back({key1, value1});

    demo_generated::PayloadBufferInput key2{};
    key2.bytes.assign({'d', 'e', 's', 'c'});
    key2.dma_safe = true;
    key2.origin = "dma-region";
    demo_generated::AttrEntryValueView value2{};
    value2.kind = demo_generated::AttrEntryValueCase::kText;
    value2.text.bytes.assign({'l', 'o', 'n', 'g', '_', 'a', 't', 't', 'r'});
    value2.text.dma_safe = true;
    value2.text.ownership_tracked = true;
    value2.text.ownership_cookie = 0x1234;
    value2.text.origin = "dma-region";
    req.attrs.push_back({key2, value2});

    req.payload_case = demo_generated::RequestPayloadCase::kRaw;
    req.raw.bytes.assign({'p','a','y','l','o','a','d','_','b','y','t','e','s','_','z','c'});
    req.raw.dma_safe = true;
    req.raw.ownership_tracked = true;
    req.raw.ownership_cookie = 0x8888;
    req.raw.origin = "dma-region";

    req.scores = {1, 3, 7, 15};
    return req;
}

}  // namespace dmesh::grpc_codegen::tests
