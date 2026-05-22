#ifndef GRPC_OFFLOAD_H
#define GRPC_OFFLOAD_H

#include <stddef.h>
#include <stdint.h>

#include <doca_error.h>

#include "dpa_common.h"

struct objects;

struct dmesh_grpc_arena_block {
	size_t offset;
	uint64_t owner_ring_seq;
	uint32_t next_free;
	uint8_t in_use;
	uint8_t active;
	uint8_t reserved[2];
};

struct dmesh_grpc_arena {
	uint8_t *base;
	size_t capacity;
	size_t block_size;
	size_t block_count;
	uint32_t free_head;
	uint32_t current_block;
	struct dmesh_grpc_arena_block *blocks;
};

doca_error_t dmesh_grpc_arena_init(struct dmesh_grpc_arena *arena, void *base, size_t capacity);
void dmesh_grpc_arena_destroy(struct dmesh_grpc_arena *arena);
void dmesh_grpc_arena_reset(struct dmesh_grpc_arena *arena);
size_t dmesh_grpc_arena_available(const struct dmesh_grpc_arena *arena);
void *dmesh_grpc_arena_alloc(struct dmesh_grpc_arena *arena, size_t size, size_t align);
void dmesh_grpc_arena_abort_request(struct dmesh_grpc_arena *arena, const void *request);
doca_error_t dmesh_grpc_arena_finish_request(struct dmesh_grpc_arena *arena,
					     const void *request,
					     uint64_t ring_seq);
void dmesh_grpc_arena_reclaim_ring_seq(struct dmesh_grpc_arena *arena, uint64_t ring_seq);
void dmesh_grpc_arena_reclaim_through(struct dmesh_grpc_arena *arena, uint64_t consumer_seq);

doca_error_t dmesh_grpc_hello_request_alloc(struct dmesh_grpc_arena *arena,
					    uint64_t id,
					    const char *name,
					    uint32_t name_len,
					    const uint32_t *scores,
					    uint32_t scores_count,
					    struct dmesh_grpc_hello_request **out);

doca_error_t dmesh_grpc_submit_request(struct objects *objs,
				       uint32_t schema_id,
				       struct dmesh_grpc_arena *arena,
				       const void *rpc_obj,
				       size_t rpc_obj_size,
				       uint32_t request_id);

doca_error_t dmesh_grpc_handle_dma_completion(struct objects *objs,
					      const struct comch_grpc_dma_comp_msg *comp);
void dmesh_grpc_dpu_state_destroy(struct objects *objs);

#endif /* GRPC_OFFLOAD_H */
