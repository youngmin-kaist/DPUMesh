#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum WireType {
    WIRE_VARINT = 0,
    WIRE_64BIT = 1,
    WIRE_LEN = 2,
    WIRE_32BIT = 5,
};

enum FieldKind {
    FK_U32 = 0,
    FK_U64,
    FK_BOOL,
    FK_STRING,
    FK_BYTES,
    FK_MESSAGE,
    FK_REPEATED_U32_PACKED,
    FK_REPEATED_U64_PACKED,
};

typedef struct {
    uint32_t offset; /* base-relative offset */
    uint32_t len;
} DpaStringRef;

typedef struct {
    uint32_t offset; /* base-relative offset */
    uint32_t count;
} DpaU32ArrayRef;

typedef struct {
    uint32_t offset; /* base-relative offset */
    uint32_t count;
} DpaU64ArrayRef;

typedef struct {
    uint32_t field_no;
    uint8_t kind;
    uint8_t reserved0;
    uint16_t reserved1;
    uint32_t offset;        /* offsetof(flat_struct, field) */
    uint32_t child_desc_id; /* used only when kind == FK_MESSAGE */
} ProtoFieldDesc;

typedef struct {
    uint32_t desc_id;
    uint32_t field_begin; /* index in ProtoDescBlob.fields[] */
    uint32_t field_count;
    uint32_t flat_size;
} ProtoMessageDesc;

#define PROTO_MAX_MSG_DESC 32
#define PROTO_MAX_FIELD_DESC 256
/* Keep protobuf length-delimited payload bounded to 2GB. */
#define PROTO_MAX_LEN_DELIMITED ((uint32_t)0x7fffffffU)

#define DMESH_GRPC_SCHEMA_HELLO_REQUEST 1U

typedef struct {
    uint32_t msg_count;
    uint32_t field_count;
    ProtoMessageDesc msgs[PROTO_MAX_MSG_DESC];
    ProtoFieldDesc fields[PROTO_MAX_FIELD_DESC];
} ProtoDescBlob;

typedef struct {
    uint64_t id;
    DpaStringRef name;
    DpaU32ArrayRef scores;
} HelloRequestFlat;

typedef struct {
    uint32_t desc_id;

    const void *host_msg_addr;
    uint32_t host_msg_len;

    void *host_out_addr;
    uint32_t host_out_cap;

    uint64_t dpa_msg_addr;
    uint64_t dpa_out_addr;
    uint32_t dpa_out_cap;

    uint32_t request_id;
} ProtoTask;

typedef struct {
    uint32_t request_id;
    uint32_t encoded_len;
    int32_t status; /* 0=OK, <0=ERR */
} ProtoCompletion;

typedef struct {
    uint64_t desc_blob_addr;
    uint64_t task_array_addr;
    uint64_t completion_array_addr;
    uint32_t max_batch;
    uint32_t shard_id;
    uint32_t shard_count;
    uint32_t reserved0;

    uint64_t ring_ctrl_addr;
    uint64_t req_ring_addr;
    uint64_t cpl_ring_addr;
    uint64_t msg_pool_addr;
    uint64_t out_pool_addr;
    uint32_t ring_depth;
    uint32_t msg_slot_size;
    uint32_t out_slot_size;
} GrpcDpaWorkerArg;

typedef struct {
    uint32_t req_head;
    uint32_t req_tail;
    uint32_t cpl_head;
    uint32_t cpl_tail;
    uint32_t shutdown;
    uint32_t reserved[3];
} GrpcRingCtrl;

typedef struct {
    uint32_t request_id;
    uint32_t desc_id;
    uint32_t msg_slot;
    uint32_t out_slot;
    uint32_t msg_len;
    uint32_t out_cap;
    uint32_t valid;
    uint32_t reserved;
} GrpcReqDesc;

typedef struct {
    uint32_t request_id;
    uint32_t encoded_len;
    int32_t status;
    uint32_t out_slot;
    uint32_t valid;
    uint32_t reserved[3];
} GrpcCplDesc;

#ifdef __cplusplus
}
#endif
