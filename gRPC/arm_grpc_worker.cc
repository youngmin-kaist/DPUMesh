#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>
#include <arpa/inet.h>

struct GrpcFrame {
    std::vector<uint8_t> bytes;
};

/* Use when payload is already generated as full gRPC frame on DPA side. */
static GrpcFrame adopt_grpc_frame(std::vector<uint8_t>&& prewrapped) {
    GrpcFrame f;
    f.bytes = std::move(prewrapped);
    return f;
}

static GrpcFrame wrap_grpc_message(const uint8_t* payload, uint32_t len) {
    GrpcFrame f;
    f.bytes.resize(5 + len);

    f.bytes[0] = 0; // not compressed

    uint32_t be_len = htonl(len);
    std::memcpy(f.bytes.data() + 1, &be_len, 4);
    std::memcpy(f.bytes.data() + 5, payload, len);

    return f;
}
