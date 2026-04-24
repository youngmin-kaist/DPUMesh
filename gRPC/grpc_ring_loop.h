#pragma once

#include "proto_meta.h"

#ifdef __cplusplus
extern "C" {
#endif

void grpc_ring_reset(void);
void grpc_ring_set_shutdown(int on);
int grpc_ring_is_shutdown(void);

int grpc_ring_push_task(const ProtoTask *task);
int grpc_ring_pop_completion(ProtoCompletion *cpl);

/* Symbols consumed by dpa_batch_worker.c when GRPC_DPA_ENABLE_RING_LOOP is enabled */
int ring_pop_task(ProtoTask *task);
void ring_push_completion(const ProtoCompletion *cpl);

#ifdef __cplusplus
}
#endif

