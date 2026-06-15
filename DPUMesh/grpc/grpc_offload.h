#ifndef GRPC_OFFLOAD_H
#define GRPC_OFFLOAD_H

#include <stddef.h>
#include <stdint.h>

#include <doca_error.h>

#include "../dpa_common.h"

#define DMESH_GRPC_ARENA_NO_BLOCK UINT32_MAX

struct objects;

struct dmesh_grpc_arena_block {
	size_t offset;
	uint64_t owner_ring_seq;
	uint32_t next_free;
	uint8_t in_use;
	uint8_t active;
	uint8_t reserved[2];
};

struct dmesh_grpc_arena_active {
	uint64_t ring_seq;
	uint32_t block_idx;
	uint32_t reserved;
};

struct dmesh_grpc_arena {
	uint8_t *base;
	size_t capacity;
	size_t block_size;
	size_t block_count;
	size_t free_count;
	size_t active_count;
	uint64_t reclaimed_seq;
	uint32_t free_head;
	struct dmesh_grpc_arena_block *blocks;
	struct dmesh_grpc_arena_active *active;
};

struct dmesh_grpc_pending_entry {
	uint64_t ring_seq;
	uint32_t request_id;
	uint16_t completed_dma;
	uint16_t expected_dma;
} __attribute__((aligned(16)));

struct dmesh_grpc_dpu_state {
	struct dmesh_grpc_pending_entry pending[DMESH_GRPC_MAX_PENDING];
};

doca_error_t dmesh_grpc_arena_init(struct dmesh_grpc_arena *arena, void *base, size_t capacity);
void dmesh_grpc_arena_destroy(struct dmesh_grpc_arena *arena);
void dmesh_grpc_arena_reset(struct dmesh_grpc_arena *arena);
size_t dmesh_grpc_arena_available(const struct dmesh_grpc_arena *arena);
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
doca_error_t dmesh_grpc_hello_flat_alloc(struct dmesh_grpc_arena *arena,
					 uint64_t id,
					 const char *name,
					 uint32_t name_len,
					 const uint32_t *scores,
					 uint32_t scores_count,
					 struct dmesh_grpc_hello_flat **out,
					 size_t *flat_len);

doca_error_t dmesh_grpc_submit_request(struct objects *objs,
				       uint32_t schema_id,
				       struct dmesh_grpc_arena *arena,
				       const void *rpc_obj,
				       size_t rpc_obj_size,
				       uint32_t request_id);

doca_error_t dmesh_grpc_handle_dma_completion(struct objects *objs,
					      struct comch_grpc_dma_comp_msg *comp);
void dmesh_grpc_dpu_state_destroy(struct objects *objs);

#endif /* GRPC_OFFLOAD_H */
