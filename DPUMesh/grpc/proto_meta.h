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
    uint32_t child_schema_id; /* used only when kind == FK_MESSAGE */
} ProtoFieldSchema;

typedef struct {
    uint32_t schema_id;
    uint32_t field_begin; /* index in ProtoSchemaBlob.fields[] */
    uint32_t field_count;
    uint32_t flat_size;
} ProtoMessageSchema;

#define PROTO_MAX_MSG_SCHEMA 32
#define PROTO_MAX_FIELD_SCHEMA 256
/* Keep protobuf length-delimited payload bounded to 2GB. */
#define PROTO_MAX_LEN_DELIMITED ((uint32_t)0x7fffffffU)

#define DMESH_GRPC_SCHEMA_HELLO_REQUEST 1U

typedef struct {
    uint32_t msg_count;
    uint32_t field_count;
    ProtoMessageSchema msgs[PROTO_MAX_MSG_SCHEMA];
    ProtoFieldSchema fields[PROTO_MAX_FIELD_SCHEMA];
} ProtoSchemaBlob;

typedef struct {
    uint64_t id;
    DpaStringRef name;
    DpaU32ArrayRef scores;
} HelloRequestFlat;

typedef struct {
    uint32_t request_id;
    uint32_t schema_id;
    uint64_t flat;
    uint64_t out;
} ProtoTask;

typedef struct {
    uint32_t request_id;
    uint32_t encoded_len;
    int32_t status; /* 0=OK, <0=ERR */
} ProtoCompletion;

#ifdef __cplusplus
}
#endif
