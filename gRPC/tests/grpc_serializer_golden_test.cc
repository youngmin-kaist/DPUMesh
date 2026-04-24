#include "../grpc_wire_encode.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

static constexpr uint32_t kDefaultRandomIters = 200U;
static constexpr uint32_t kDefaultRandomSeed = 12345U;

static size_t align_up(size_t value, size_t align)
{
    return (value + align - 1U) & ~(align - 1U);
}

static uint32_t read_env_u32(const char *name, uint32_t fallback)
{
    const char *s = std::getenv(name);
    char *end = nullptr;
    unsigned long v;

    if (s == nullptr || *s == '\0')
        return fallback;

    v = std::strtoul(s, &end, 10);
    if (end == s || *end != '\0' || v > 0xffffffffUL)
        return fallback;
    return static_cast<uint32_t>(v);
}

static void dump_hex(const char *label, const std::vector<uint8_t> &bytes)
{
    std::fprintf(stderr, "%s:", label);
    for (uint8_t b : bytes)
        std::fprintf(stderr, " %02x", static_cast<unsigned>(b));
    std::fprintf(stderr, "\n");
}

static bool expect_bytes(const char *name,
                         const std::vector<uint8_t> &actual,
                         const std::vector<uint8_t> &expected)
{
    if (actual == expected)
        return true;

    std::fprintf(stderr, "FAIL %s: actual_len=%zu expected_len=%zu\n",
                 name, actual.size(), expected.size());
    dump_hex("actual", actual);
    dump_hex("expected", expected);
    return false;
}

static void append_varint(std::vector<uint8_t> &out, uint64_t v)
{
    while (v >= 0x80U) {
        out.push_back(static_cast<uint8_t>((v & 0x7fU) | 0x80U));
        v >>= 7U;
    }
    out.push_back(static_cast<uint8_t>(v));
}

static void append_tag(std::vector<uint8_t> &out, uint32_t field_no, uint8_t wire_type)
{
    append_varint(out, (static_cast<uint64_t>(field_no) << 3U) | wire_type);
}

static std::vector<uint8_t> add_grpc_header(const std::vector<uint8_t> &payload)
{
    std::vector<uint8_t> framed;

    framed.reserve(payload.size() + 5U);
    framed.push_back(0U);
    framed.push_back(static_cast<uint8_t>((payload.size() >> 24U) & 0xffU));
    framed.push_back(static_cast<uint8_t>((payload.size() >> 16U) & 0xffU));
    framed.push_back(static_cast<uint8_t>((payload.size() >> 8U) & 0xffU));
    framed.push_back(static_cast<uint8_t>(payload.size() & 0xffU));
    framed.insert(framed.end(), payload.begin(), payload.end());
    return framed;
}

static std::vector<uint8_t> reference_hello_bytes(uint64_t id,
                                                  const std::string &name,
                                                  const std::vector<uint32_t> &scores)
{
    std::vector<uint8_t> payload;
    std::vector<uint8_t> packed;

    if (id != 0U) {
        append_tag(payload, 1U, WIRE_VARINT);
        append_varint(payload, id);
    }

    if (!name.empty()) {
        append_tag(payload, 2U, WIRE_LEN);
        append_varint(payload, name.size());
        payload.insert(payload.end(), name.begin(), name.end());
    }

    if (!scores.empty()) {
        for (uint32_t v : scores)
            append_varint(packed, v);
        append_tag(payload, 3U, WIRE_LEN);
        append_varint(payload, packed.size());
        payload.insert(payload.end(), packed.begin(), packed.end());
    }

    return add_grpc_header(payload);
}

static std::vector<uint8_t> reference_nested_bytes(uint32_t a, const std::string &b)
{
    std::vector<uint8_t> inner;
    std::vector<uint8_t> outer;

    if (a != 0U) {
        append_tag(inner, 1U, WIRE_VARINT);
        append_varint(inner, a);
    }

    if (!b.empty()) {
        append_tag(inner, 2U, WIRE_LEN);
        append_varint(inner, b.size());
        inner.insert(inner.end(), b.begin(), b.end());
    }

    append_tag(outer, 1U, WIRE_LEN);
    append_varint(outer, inner.size());
    outer.insert(outer.end(), inner.begin(), inner.end());
    return add_grpc_header(outer);
}

static ProtoDescBlob build_hello_desc_blob()
{
    ProtoDescBlob blob{};

    blob.msg_count = 1;
    blob.field_count = 3;
    blob.msgs[0].desc_id = 1;
    blob.msgs[0].field_begin = 0;
    blob.msgs[0].field_count = 3;
    blob.msgs[0].flat_size = sizeof(HelloRequestFlat);

    blob.fields[0].field_no = 1;
    blob.fields[0].kind = FK_U64;
    blob.fields[0].offset = static_cast<uint32_t>(offsetof(HelloRequestFlat, id));

    blob.fields[1].field_no = 2;
    blob.fields[1].kind = FK_STRING;
    blob.fields[1].offset = static_cast<uint32_t>(offsetof(HelloRequestFlat, name));

    blob.fields[2].field_no = 3;
    blob.fields[2].kind = FK_REPEATED_U32_PACKED;
    blob.fields[2].offset = static_cast<uint32_t>(offsetof(HelloRequestFlat, scores));

    return blob;
}

struct InnerFlatTest {
    uint32_t a;
    DpaStringRef b;
};

struct OuterFlatTest {
    uint32_t child_off;
};

static ProtoDescBlob build_nested_desc_blob()
{
    ProtoDescBlob blob{};

    blob.msg_count = 2;
    blob.field_count = 3;

    blob.msgs[0].desc_id = 1;
    blob.msgs[0].field_begin = 0;
    blob.msgs[0].field_count = 1;
    blob.msgs[0].flat_size = sizeof(OuterFlatTest);

    blob.msgs[1].desc_id = 2;
    blob.msgs[1].field_begin = 1;
    blob.msgs[1].field_count = 2;
    blob.msgs[1].flat_size = sizeof(InnerFlatTest);

    blob.fields[0].field_no = 1;
    blob.fields[0].kind = FK_MESSAGE;
    blob.fields[0].offset = static_cast<uint32_t>(offsetof(OuterFlatTest, child_off));
    blob.fields[0].child_desc_id = 2;

    blob.fields[1].field_no = 1;
    blob.fields[1].kind = FK_U32;
    blob.fields[1].offset = static_cast<uint32_t>(offsetof(InnerFlatTest, a));

    blob.fields[2].field_no = 2;
    blob.fields[2].kind = FK_STRING;
    blob.fields[2].offset = static_cast<uint32_t>(offsetof(InnerFlatTest, b));

    return blob;
}

static bool serialize_hello_case(const ProtoDescBlob &blob,
                                 uint64_t id,
                                 const std::string &name,
                                 const std::vector<uint32_t> &scores,
                                 std::vector<uint8_t> &actual,
                                 GrpcWireEncodeStats *stats)
{
    std::vector<uint8_t> arena;
    ProtoTask task{};
    ProtoCompletion cpl{};
    size_t name_off = sizeof(HelloRequestFlat);
    size_t scores_off = align_up(name_off + name.size(), alignof(uint32_t));
    HelloRequestFlat *msg = nullptr;
    uint8_t out[8192] = {0};

    arena.resize(scores_off + scores.size() * sizeof(uint32_t), 0U);
    msg = reinterpret_cast<HelloRequestFlat *>(arena.data());
    msg->id = id;
    msg->name.offset = static_cast<uint32_t>(name_off);
    msg->name.len = static_cast<uint32_t>(name.size());
    msg->scores.offset = static_cast<uint32_t>(scores_off);
    msg->scores.count = static_cast<uint32_t>(scores.size());

    if (!name.empty())
        std::memcpy(arena.data() + name_off, name.data(), name.size());
    if (!scores.empty())
        std::memcpy(arena.data() + scores_off, scores.data(), scores.size() * sizeof(uint32_t));

    task.desc_id = 1U;
    task.dpa_msg_addr = reinterpret_cast<uint64_t>(arena.data());
    task.dpa_out_addr = reinterpret_cast<uint64_t>(out);
    task.dpa_out_cap = sizeof(out);
    task.request_id = 1U;

    if (stats != nullptr)
        grpc_wire_encode_stats_reset(stats);
    if (grpc_wire_serialize_one(&blob, &task, &cpl, stats) != 0)
        return false;

    actual.assign(out, out + cpl.encoded_len);
    return true;
}

static bool serialize_nested_case(const ProtoDescBlob &blob,
                                  uint32_t a,
                                  const std::string &b,
                                  std::vector<uint8_t> &actual,
                                  GrpcWireEncodeStats *stats)
{
    std::vector<uint8_t> arena;
    ProtoTask task{};
    ProtoCompletion cpl{};
    size_t outer_off = 0U;
    size_t inner_off = sizeof(OuterFlatTest);
    size_t text_off = inner_off + sizeof(InnerFlatTest);
    OuterFlatTest *outer = nullptr;
    InnerFlatTest *inner = nullptr;
    uint8_t out[8192] = {0};

    arena.resize(text_off + b.size(), 0U);
    outer = reinterpret_cast<OuterFlatTest *>(arena.data() + outer_off);
    inner = reinterpret_cast<InnerFlatTest *>(arena.data() + inner_off);
    outer->child_off = static_cast<uint32_t>(inner_off);
    inner->a = a;
    inner->b.offset = static_cast<uint32_t>(text_off - inner_off);
    inner->b.len = static_cast<uint32_t>(b.size());

    if (!b.empty())
        std::memcpy(arena.data() + text_off, b.data(), b.size());

    task.desc_id = 1U;
    task.dpa_msg_addr = reinterpret_cast<uint64_t>(arena.data());
    task.dpa_out_addr = reinterpret_cast<uint64_t>(out);
    task.dpa_out_cap = sizeof(out);
    task.request_id = 2U;

    if (stats != nullptr)
        grpc_wire_encode_stats_reset(stats);
    if (grpc_wire_serialize_one(&blob, &task, &cpl, stats) != 0)
        return false;

    actual.assign(out, out + cpl.encoded_len);
    return true;
}

static std::string random_ascii(std::mt19937 &rng, uint32_t len)
{
    static constexpr char alphabet[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";
    std::uniform_int_distribution<size_t> pick(0U, sizeof(alphabet) - 2U);
    std::string out;

    out.resize(len);
    for (uint32_t i = 0; i < len; ++i)
        out[i] = alphabet[pick(rng)];
    return out;
}

static bool run_hello_mixed_case()
{
    ProtoDescBlob blob = build_hello_desc_blob();
    GrpcWireEncodeStats stats{};
    std::vector<uint8_t> actual;
    const std::vector<uint8_t> expected = {
        0x00, 0x00, 0x00, 0x00, 0x0f,
        0x08, 0x96, 0x01,
        0x12, 0x03, 0x61, 0x62, 0x63,
        0x1a, 0x05, 0x01, 0x96, 0x01, 0xac, 0x02,
    };

    if (!serialize_hello_case(blob, 150U, "abc", {1U, 150U, 300U}, actual, &stats))
        return false;
    if (!expect_bytes("hello_mixed", actual, expected))
        return false;
    return stats.specialized_hits == 1U;
}

static bool run_packed_multibyte_len_case()
{
    ProtoDescBlob blob = build_hello_desc_blob();
    std::vector<uint32_t> packed_scores(130U, 1U);
    std::vector<uint8_t> actual;

    if (!serialize_hello_case(blob, 0U, "", packed_scores, actual, nullptr))
        return false;
    if (actual.size() != 5U + 1U + 2U + 130U)
        return false;
    return actual[5] == 0x1aU && actual[6] == 0x82U && actual[7] == 0x01U;
}

static bool run_nested_case()
{
    ProtoDescBlob blob = build_nested_desc_blob();
    GrpcWireEncodeStats stats{};
    std::vector<uint8_t> actual;
    const std::vector<uint8_t> expected = {
        0x00, 0x00, 0x00, 0x00, 0x08,
        0x0a, 0x06, 0x08, 0x96, 0x01, 0x12, 0x01, 0x7a,
    };

    if (!serialize_nested_case(blob, 150U, "z", actual, &stats))
        return false;
    if (!expect_bytes("nested_len_varint", actual, expected))
        return false;
    return stats.generic_fallbacks != 0U;
}

static bool run_random_cases(uint32_t iters, uint32_t seed)
{
    ProtoDescBlob hello_blob = build_hello_desc_blob();
    ProtoDescBlob nested_blob = build_nested_desc_blob();
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint64_t> id_dist(0U, 1ULL << 28U);
    std::uniform_int_distribution<uint32_t> name_len_dist(0U, 96U);
    std::uniform_int_distribution<uint32_t> score_count_dist(0U, 320U);
    std::uniform_int_distribution<uint32_t> score_val_dist(0U, 1U << 21U);
    std::uniform_int_distribution<uint32_t> nested_a_dist(0U, 1U << 20U);
    std::uniform_int_distribution<uint32_t> nested_len_dist(0U, 64U);
    uint32_t i;

    for (i = 0; i < iters; ++i) {
        {
            std::string name = random_ascii(rng, name_len_dist(rng));
            std::vector<uint32_t> scores(score_count_dist(rng));
            std::vector<uint8_t> actual;
            std::vector<uint8_t> expected;
            uint64_t id = id_dist(rng);

            for (uint32_t &v : scores)
                v = score_val_dist(rng);
            if (!serialize_hello_case(hello_blob, id, name, scores, actual, nullptr))
                return false;
            expected = reference_hello_bytes(id, name, scores);
            if (!expect_bytes("random_hello_case", actual, expected))
                return false;
        }

        {
            std::vector<uint32_t> scores(score_count_dist(rng));
            std::vector<uint8_t> actual;
            std::vector<uint8_t> expected;

            for (uint32_t &v : scores)
                v = score_val_dist(rng);
            if (!serialize_hello_case(hello_blob, 0U, "", scores, actual, nullptr))
                return false;
            expected = reference_hello_bytes(0U, "", scores);
            if (!expect_bytes("random_packed_case", actual, expected))
                return false;
        }

        {
            std::string text = random_ascii(rng, nested_len_dist(rng));
            std::vector<uint8_t> actual;
            std::vector<uint8_t> expected;
            GrpcWireEncodeStats stats{};
            uint32_t a = nested_a_dist(rng);

            if (!serialize_nested_case(nested_blob, a, text, actual, &stats))
                return false;
            expected = reference_nested_bytes(a, text);
            if (!expect_bytes("random_nested_case", actual, expected))
                return false;
            if (stats.generic_fallbacks == 0U)
                return false;
        }
    }

    return true;
}

}  // namespace

int main()
{
    const uint32_t random_iters = read_env_u32("GRPC_SERIALIZER_RANDOM_ITERS", kDefaultRandomIters);
    const uint32_t random_seed = read_env_u32("GRPC_SERIALIZER_RANDOM_SEED", kDefaultRandomSeed);

    if (!run_hello_mixed_case())
        return 1;
    if (!run_packed_multibyte_len_case())
        return 1;
    if (!run_nested_case())
        return 1;
    if (!run_random_cases(random_iters, random_seed))
        return 1;

    std::printf("grpc_serializer_golden_test: ok (random_iters=%u seed=%u)\n",
                random_iters,
                random_seed);
    return 0;
}
