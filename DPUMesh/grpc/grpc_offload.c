#include "grpc_offload.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include <doca_log.h>
#include <doca_mmap.h>

#include "../dpa.h"
#include "../object.h"
#include "../ring.h"

DOCA_LOG_REGISTER(GRPC_OFFLOAD);

struct dmesh_grpc_arena_txn {
	struct dmesh_grpc_arena *arena;
	uint32_t block_idx;
	size_t offset;
	bool committed;
};

static size_t
align_up_size(size_t value, size_t align)
{
	if (align == 0)
		return value;
	if (value > SIZE_MAX - (align - 1U))
		return SIZE_MAX;
	return (value + align - 1U) & ~(align - 1U);
}

static uintptr_t
align_up_uintptr(uintptr_t value, size_t align)
{
	if (align == 0)
		return value;
	if (value > UINTPTR_MAX - (uintptr_t)(align - 1U))
		return UINTPTR_MAX;
	return (value + (uintptr_t)(align - 1U)) & ~((uintptr_t)align - 1U);
}

static void
dmesh_grpc_arena_link_free_block(struct dmesh_grpc_arena *arena, uint32_t block_idx)
{
	struct dmesh_grpc_arena_block *block;

	if (arena == NULL || arena->blocks == NULL || block_idx >= arena->block_count)
		return;

	block = &arena->blocks[block_idx];
	block->offset = 0;
	block->owner_ring_seq = 0;
	block->in_use = 0;
	block->active = 0;
	block->next_free = arena->free_head;
	arena->free_head = block_idx;
	arena->free_count++;
}

static void
dmesh_grpc_arena_release_block(struct dmesh_grpc_arena *arena, uint32_t block_idx)
{
	struct dmesh_grpc_arena_block *block;

	if (arena == NULL || arena->blocks == NULL || block_idx >= arena->block_count)
		return;

	block = &arena->blocks[block_idx];
	if (block->in_use == 0 && block->active == 0)
		return;

	dmesh_grpc_arena_link_free_block(arena, block_idx);
}

static uint32_t
dmesh_grpc_arena_pop_block(struct dmesh_grpc_arena *arena)
{
	uint32_t block_idx;
	struct dmesh_grpc_arena_block *block;

	if (arena == NULL || arena->free_head == DMESH_GRPC_ARENA_NO_BLOCK)
		return DMESH_GRPC_ARENA_NO_BLOCK;

	block_idx = arena->free_head;
	block = &arena->blocks[block_idx];
	arena->free_head = block->next_free;
	if (arena->free_count != 0)
		arena->free_count--;
	block->offset = 0;
	block->owner_ring_seq = 0;
	block->in_use = 1;
	block->active = 0;
	block->next_free = DMESH_GRPC_ARENA_NO_BLOCK;
	return block_idx;
}

static size_t
dmesh_grpc_arena_active_slot(const struct dmesh_grpc_arena *arena, uint64_t ring_seq)
{
	return (size_t)(ring_seq % arena->active_count);
}

static uint32_t
dmesh_grpc_arena_block_for_ptr(const struct dmesh_grpc_arena *arena, const void *ptr)
{
	uintptr_t base;
	uintptr_t cur;
	size_t offset;
	uint32_t block_idx;

	if (arena == NULL || arena->base == NULL || ptr == NULL || arena->block_size == 0)
		return DMESH_GRPC_ARENA_NO_BLOCK;

	base = (uintptr_t)arena->base;
	cur = (uintptr_t)ptr;
	if (cur < base || cur >= base + arena->capacity)
		return DMESH_GRPC_ARENA_NO_BLOCK;
	offset = (size_t)(cur - base);
	block_idx = (uint32_t)(offset / arena->block_size);
	if (block_idx >= arena->block_count)
		return DMESH_GRPC_ARENA_NO_BLOCK;
	return block_idx;
}

static doca_error_t
dmesh_grpc_arena_txn_begin(struct dmesh_grpc_arena *arena,
			   struct dmesh_grpc_arena_txn *txn)
{
	uint32_t block_idx;

	if (arena == NULL || arena->base == NULL || arena->blocks == NULL ||
	    txn == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	block_idx = dmesh_grpc_arena_pop_block(arena);
	if (block_idx == DMESH_GRPC_ARENA_NO_BLOCK)
		return DOCA_ERROR_NO_MEMORY;

	*txn = (struct dmesh_grpc_arena_txn){
		.arena = arena,
		.block_idx = block_idx,
		.offset = 0,
		.committed = false,
	};
	return DOCA_SUCCESS;
}

static void *
dmesh_grpc_arena_txn_alloc(struct dmesh_grpc_arena_txn *txn, size_t size,
			   size_t align, bool zero_padding)
{
	struct dmesh_grpc_arena *arena;
	struct dmesh_grpc_arena_block *block;
	size_t off;
	size_t alloc_size;
	size_t block_base_off;
	uintptr_t base_addr;
	uintptr_t raw_addr;
	uintptr_t aligned_addr;

	if (txn == NULL || txn->arena == NULL || txn->committed || size == 0)
		return NULL;

	arena = txn->arena;
	if (txn->block_idx >= arena->block_count)
		return NULL;

	if (align < DMESH_GRPC_ARENA_ADDR_ALIGN)
		align = DMESH_GRPC_ARENA_ADDR_ALIGN;

	block = &arena->blocks[txn->block_idx];
	if (block->in_use == 0 || block->active != 0)
		return NULL;

	block_base_off = (size_t)txn->block_idx * arena->block_size;
	base_addr = (uintptr_t)(arena->base + block_base_off);
	raw_addr = base_addr + txn->offset;
	aligned_addr = align_up_uintptr(raw_addr, align);
	if (aligned_addr < base_addr)
		return NULL;

	off = (size_t)(aligned_addr - base_addr);
	alloc_size = align_up_size(size, DMESH_GRPC_DMA_MSG_ALIGN);
	if (alloc_size == SIZE_MAX || off > arena->block_size ||
	    alloc_size > arena->block_size - off)
		return NULL;

	txn->offset = off + alloc_size;
	block->offset = txn->offset;
	if (zero_padding && alloc_size > size)
		memset((uint8_t *)aligned_addr + size, 0, alloc_size - size);
	return (void *)aligned_addr;
}

static doca_error_t
dmesh_grpc_arena_txn_commit(struct dmesh_grpc_arena_txn *txn, const void *request)
{
	struct dmesh_grpc_arena *arena;

	if (txn == NULL || txn->arena == NULL || request == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	arena = txn->arena;
	if (dmesh_grpc_arena_block_for_ptr(arena, request) != txn->block_idx)
		return DOCA_ERROR_BAD_STATE;

	txn->committed = true;
	return DOCA_SUCCESS;
}

static void
dmesh_grpc_arena_txn_abort(struct dmesh_grpc_arena_txn *txn)
{
	if (txn == NULL || txn->arena == NULL || txn->committed)
		return;

	dmesh_grpc_arena_release_block(txn->arena, txn->block_idx);
	txn->committed = true;
}

doca_error_t
dmesh_grpc_arena_init(struct dmesh_grpc_arena *arena, void *base, size_t capacity)
{
	uint32_t i;

	if (arena == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	memset(arena, 0, sizeof(*arena));
	if (base == NULL || capacity < DMA_MSG_SIZE_LIMIT)
		return DOCA_ERROR_INVALID_VALUE;

	arena->base = (uint8_t *)base;
	arena->block_size = DMA_MSG_SIZE_LIMIT;
	arena->block_count = capacity / DMA_MSG_SIZE_LIMIT;
	if (arena->block_count > UINT32_MAX)
		return DOCA_ERROR_INVALID_VALUE;
	arena->capacity = capacity;
	arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
	arena->blocks = calloc(arena->block_count, sizeof(*arena->blocks));
	if (arena->blocks == NULL)
		return DOCA_ERROR_NO_MEMORY;
	arena->active_count = arena->block_count;
	arena->active = calloc(arena->active_count, sizeof(*arena->active));
	if (arena->active == NULL) {
		free(arena->blocks);
		memset(arena, 0, sizeof(*arena));
		arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
		return DOCA_ERROR_NO_MEMORY;
	}

	for (i = 0; i < arena->block_count; ++i)
		dmesh_grpc_arena_link_free_block(arena, arena->block_count - 1U - i);
	return DOCA_SUCCESS;
}

void
dmesh_grpc_arena_destroy(struct dmesh_grpc_arena *arena)
{
	if (arena == NULL)
		return;
	free(arena->blocks);
	free(arena->active);
	memset(arena, 0, sizeof(*arena));
	arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
}

void
dmesh_grpc_arena_reset(struct dmesh_grpc_arena *arena)
{
	uint32_t i;

	if (arena == NULL)
		return;
	arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
	arena->free_count = 0;
	arena->reclaimed_seq = 0;
	if (arena->blocks != NULL)
		memset(arena->blocks, 0, arena->block_count * sizeof(*arena->blocks));
	if (arena->active != NULL)
		memset(arena->active, 0, arena->active_count * sizeof(*arena->active));
	for (i = 0; i < arena->block_count; ++i)
		dmesh_grpc_arena_link_free_block(arena, arena->block_count - 1U - i);
}

size_t
dmesh_grpc_arena_available(const struct dmesh_grpc_arena *arena)
{
	if (arena == NULL || arena->blocks == NULL)
		return 0;
	return arena->free_count * arena->block_size;
}

void
dmesh_grpc_arena_abort_request(struct dmesh_grpc_arena *arena, const void *request)
{
	uint32_t block_idx;
	struct dmesh_grpc_arena_block *block;

	block_idx = dmesh_grpc_arena_block_for_ptr(arena, request);
	if (block_idx == DMESH_GRPC_ARENA_NO_BLOCK)
		return;
	block = &arena->blocks[block_idx];
	if (block->active != 0)
		return;
	if (block->in_use == 0)
		return;
	dmesh_grpc_arena_release_block(arena, block_idx);
}

doca_error_t
dmesh_grpc_arena_finish_request(struct dmesh_grpc_arena *arena,
				const void *request,
				uint64_t ring_seq)
{
	uint32_t block_idx;
	struct dmesh_grpc_arena_block *block;
	struct dmesh_grpc_arena_active *entry;
	size_t slot;

	if (arena == NULL || arena->active == NULL || arena->active_count == 0 ||
	    ring_seq == 0)
		return DOCA_ERROR_INVALID_VALUE;
	if (ring_seq <= arena->reclaimed_seq)
		return DOCA_ERROR_BAD_STATE;

	block_idx = dmesh_grpc_arena_block_for_ptr(arena, request);
	if (block_idx == DMESH_GRPC_ARENA_NO_BLOCK)
		return DOCA_ERROR_INVALID_VALUE;

	block = &arena->blocks[block_idx];
	if (block->in_use == 0 || block->active != 0)
		return DOCA_ERROR_BAD_STATE;

	/*
	 * Ownership assumption: one host worker owns this arena metadata. DPA may
	 * keep DMA reads against this host block until it publishes consumer_seq
	 * past ring_seq; host must not recycle the block before reclaim observes
	 * that sequence. Add locking/atomics here if multiple host workers share an
	 * arena.
	 */
	slot = dmesh_grpc_arena_active_slot(arena, ring_seq);
	entry = &arena->active[slot];
	if (entry->ring_seq != 0)
		return DOCA_ERROR_BAD_STATE;

	entry->ring_seq = ring_seq;
	entry->block_idx = block_idx;
	block->owner_ring_seq = ring_seq;
	block->active = 1;
	return DOCA_SUCCESS;
}

void
dmesh_grpc_arena_reclaim_ring_seq(struct dmesh_grpc_arena *arena, uint64_t ring_seq)
{
	struct dmesh_grpc_arena_active *entry;
	size_t slot;
	uint32_t block_idx;

	if (arena == NULL || arena->blocks == NULL || arena->active == NULL ||
	    arena->active_count == 0 || ring_seq == 0)
		return;
	if (ring_seq <= arena->reclaimed_seq)
		return;

	slot = dmesh_grpc_arena_active_slot(arena, ring_seq);
	entry = &arena->active[slot];
	if (entry->ring_seq != ring_seq)
		return;

	block_idx = entry->block_idx;
	entry->ring_seq = 0;
	entry->block_idx = DMESH_GRPC_ARENA_NO_BLOCK;
	dmesh_grpc_arena_release_block(arena, block_idx);
	if (ring_seq == arena->reclaimed_seq + 1U)
		arena->reclaimed_seq = ring_seq;
}

void
dmesh_grpc_arena_reclaim_through(struct dmesh_grpc_arena *arena, uint64_t consumer_seq)
{
	struct dmesh_grpc_arena_active *entry;
	uint64_t seq;
	size_t slot;
	uint32_t block_idx;

	if (arena == NULL || arena->blocks == NULL || arena->active == NULL ||
	    arena->active_count == 0 || consumer_seq == 0)
		return;
	if (consumer_seq <= arena->reclaimed_seq)
		return;

	/*
	 * consumer_seq is published by DPA only after the serialize completion
	 * message is posted and the descriptor slot is released. This assumes the
	 * host observes that PCI write before reclaiming owner_ring_seq blocks.
	 * Only advance reclaimed_seq over active entries we registered locally; the
	 * host can briefly observe non-zero initial/test consumer_seq values before
	 * the DPA thread resets the shared consumer state.
	 */
	for (seq = arena->reclaimed_seq + 1U; seq <= consumer_seq; ++seq) {
		slot = dmesh_grpc_arena_active_slot(arena, seq);
		entry = &arena->active[slot];
		if (entry->ring_seq != seq)
			break;

		block_idx = entry->block_idx;
		entry->ring_seq = 0;
		entry->block_idx = DMESH_GRPC_ARENA_NO_BLOCK;
		dmesh_grpc_arena_release_block(arena, block_idx);
		arena->reclaimed_seq = seq;
	}
}


// per-field DMA version
doca_error_t
dmesh_grpc_hello_request_alloc(struct dmesh_grpc_arena *arena,
			       uint64_t id,
			       const char *name,
			       uint32_t name_len,
			       const uint32_t *scores,
			       uint32_t scores_count,
			       struct dmesh_grpc_hello_request **out)
{
	struct dmesh_grpc_arena_txn txn = {0};
	struct dmesh_grpc_hello_request *req;
	doca_error_t result;

	if (arena == NULL || out == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if ((name_len != 0 && name == NULL) || (scores_count != 0 && scores == NULL))
		return DOCA_ERROR_INVALID_VALUE;

	*out = NULL;

	result = dmesh_grpc_arena_txn_begin(arena, &txn);
	if (result != DOCA_SUCCESS)
		return result;

	req = dmesh_grpc_arena_txn_alloc(&txn, sizeof(*req),
					 DMESH_GRPC_ARENA_ADDR_ALIGN, true);
	if (req == NULL)
		goto no_memory;

	memset(req, 0, sizeof(*req));
	req->id = id;

	if (name_len != 0) {
		char *name_dst = dmesh_grpc_arena_txn_alloc(&txn, name_len,
							    DMESH_GRPC_ARENA_ADDR_ALIGN,
							    true);
		if (name_dst == NULL)
			goto no_memory;
		memcpy(name_dst, name, name_len);
		req->name = name_dst;
	}
	else {
		req->name = NULL;
	}
	req->name_len = name_len;
	
	if (scores_count != 0) {
		if (scores_count > SIZE_MAX / sizeof(uint32_t))
			goto invalid_value;
		size_t scores_bytes = (size_t)scores_count * sizeof(uint32_t);
		uint32_t *scores_dst = dmesh_grpc_arena_txn_alloc(&txn, scores_bytes,
								  DMESH_GRPC_ARENA_ADDR_ALIGN,
								  true);
		if (scores_dst == NULL)
			goto no_memory;
		memcpy(scores_dst, scores, scores_bytes);
		req->scores = scores_dst;
	}
	else {
		req->scores = NULL;
	}
	req->scores_count = scores_count;

	result = dmesh_grpc_arena_txn_commit(&txn, req);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_txn_abort(&txn);
		return result;
	}

	*out = req;
	return DOCA_SUCCESS;

no_memory:
	dmesh_grpc_arena_txn_abort(&txn);
	return DOCA_ERROR_NO_MEMORY;

invalid_value:
	dmesh_grpc_arena_txn_abort(&txn);
	return DOCA_ERROR_INVALID_VALUE;
}

doca_error_t
dmesh_grpc_hello_flat_alloc(struct dmesh_grpc_arena *arena,
			    uint64_t id,
			    const char *name,
			    uint32_t name_len,
			    const uint32_t *scores,
			    uint32_t scores_count,
			    struct dmesh_grpc_hello_flat **out,
			    size_t *flat_len_out)
{
	struct dmesh_grpc_arena_txn txn = {0};
	struct dmesh_grpc_hello_flat *flat;
	size_t raw_len;
	size_t flat_len;
	size_t scores_bytes = 0;
	uint32_t name_off = 0;
	uint32_t scores_off = 0;
	doca_error_t result;

	if (arena == NULL || out == NULL || flat_len_out == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if ((name_len != 0 && name == NULL) || (scores_count != 0 && scores == NULL))
		return DOCA_ERROR_INVALID_VALUE;

	*out = NULL;
	*flat_len_out = 0;

	raw_len = sizeof(*flat);

	if (name_len != 0) {
		name_off = (uint32_t)raw_len;
		if ((size_t)name_len > SIZE_MAX - raw_len)
			return DOCA_ERROR_INVALID_VALUE;
		raw_len += name_len;
	}

	if (scores_count != 0) {
		if (scores_count > SIZE_MAX / sizeof(uint32_t))
			return DOCA_ERROR_INVALID_VALUE;
		scores_bytes = (size_t)scores_count * sizeof(uint32_t);
		scores_off = (uint32_t)raw_len;
		if (scores_bytes > SIZE_MAX - raw_len)
			return DOCA_ERROR_INVALID_VALUE;
		raw_len += scores_bytes;
	}

	flat_len = align_up_size(raw_len, DMESH_GRPC_DMA_MSG_ALIGN);
	if (flat_len > DMESH_GRPC_MAX_FLAT_SIZE) {
		DOCA_LOG_INFO("flat_len %zu exceeds max limit %u", flat_len, DMESH_GRPC_MAX_FLAT_SIZE);
		return DOCA_ERROR_INVALID_VALUE;
	}

	result = dmesh_grpc_arena_txn_begin(arena, &txn);
	if (result != DOCA_SUCCESS)
		return result;

	flat = dmesh_grpc_arena_txn_alloc(&txn, flat_len,
					  DMESH_GRPC_ARENA_ADDR_ALIGN, false);
	if (flat == NULL) {
		dmesh_grpc_arena_txn_abort(&txn);
		return DOCA_ERROR_NO_MEMORY;
	}

	flat->id = id;
	flat->name.offset = name_off;
	flat->name.len = name_len;
	flat->scores.offset = scores_off;
	flat->scores.count = scores_count;

	if (name_len != 0)
		memcpy((uint8_t *)flat + name_off, name, name_len);
	if (scores_bytes != 0)
		memcpy((uint8_t *)flat + scores_off, scores, scores_bytes);
	if (flat_len > raw_len)
		memset((uint8_t *)flat + raw_len, 0, flat_len - raw_len);

	result = dmesh_grpc_arena_txn_commit(&txn, flat);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_txn_abort(&txn);
		return result;
	}

	*out = flat;
	*flat_len_out = flat_len;
	return DOCA_SUCCESS;
}

static bool
ptr_in_dma_buffer(const struct objects *objs, const void *ptr, size_t size)
{
	uintptr_t base;
	uintptr_t cur;

	if (objs == NULL || objs->dma_buffer == NULL || ptr == NULL)
		return false;
	base = (uintptr_t)objs->dma_buffer;
	cur = (uintptr_t)ptr;
	if (cur < base)
		return false;
	return (cur + size) <= (base + BUFFER_SIZE);
}

doca_error_t
dmesh_grpc_submit_request(struct objects *objs,
			  uint32_t schema_id,
			  struct dmesh_grpc_arena *arena,
			  const void *rpc_obj,
			  size_t rpc_obj_size,
			  uint32_t request_id)
{
	struct grpc_req_desc *desc;
	struct dma_ring *ring;
	uint64_t next_producer_seq;
	doca_dpa_dev_mmap_t local_mmap;
	doca_error_t result;

	if (objs == NULL || objs->dma_ring == NULL || objs->local_mmap == NULL ||
	    arena == NULL || rpc_obj == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if (!ptr_in_dma_buffer(objs, rpc_obj, rpc_obj_size)) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		return DOCA_ERROR_INVALID_VALUE;
	}

	ring = objs->dma_ring;
	while (!dma_ring_has_free_slot(ring)) {
		dma_ring_refresh_consumer(ring);
	}

	result = doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &local_mmap);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		return result;
	}


	// get new request descriptor and fill it out
	desc = get_grpc_req_desc_for_seq(ring, ring->producer_seq);
	memset(desc, 0, sizeof(*desc));
	desc->addr = (uint64_t)(uintptr_t)rpc_obj;
	desc->size = rpc_obj_size;
	desc->req_id = request_id;
	desc->mmap = local_mmap;
	desc->schema_id = (uint16_t)schema_id;

	next_producer_seq = ring->producer_seq + 1;
	result = dmesh_grpc_arena_finish_request(arena, rpc_obj, next_producer_seq);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		return result;
	}

	/* Publish the descriptor by advancing producer_seq. DPA will observe the new
	 * producer_seq and read the descriptor after it reads the updated ring tail.
	 * Host must ensure all descriptor writes are visible before updating producer_seq
	 * and the producer_seq update is visible before DPA observes it.
	 */
	desc->seq = next_producer_seq;
	ring->producer_seq = next_producer_seq;
	return DOCA_SUCCESS;
}

/* DPU */ 
static doca_error_t
send_serialize_request(struct objects *objs, struct comch_grpc_dma_comp_msg *comp)
{
	if (objs == NULL || objs->dpa_comch == NULL || comp == NULL)
		return DOCA_ERROR_INVALID_VALUE;

#if DEBUG_LOG
	if (comp->ring_seq <= 8U || (comp->ring_seq % DEBUG_INTERVAL) == 0U) {
		DOCA_LOG_INFO("gRPC serialize request: req=%u seq=%lu schema=%u len=%u expected_dma=%u",
			      comp->request_id, comp->ring_seq, comp->schema_id, comp->len, comp->expected_dma);
	}
#endif

	return dmesh_doca_dpa_msgq_pending_push(&objs->dpa_comch->send, comp);
}

static doca_error_t
get_dpu_state(struct objects *objs, struct dmesh_grpc_dpu_state **state)
{
	if (objs == NULL || state == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	if (objs->grpc_dpu_state == NULL) {
		objs->grpc_dpu_state = calloc(1, sizeof(*objs->grpc_dpu_state));
		if (objs->grpc_dpu_state == NULL)
			return DOCA_ERROR_NO_MEMORY;
	}

	*state = objs->grpc_dpu_state;
	return DOCA_SUCCESS;
}

doca_error_t
dmesh_grpc_handle_dma_completion(struct objects *objs,
				 struct comch_grpc_dma_comp_msg *comp)
{
	struct dmesh_grpc_dpu_state *state;
	struct dmesh_grpc_pending_entry *entry;
	uint32_t slot;
	doca_error_t result;

	if (objs == NULL || comp == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if (comp->ring_seq == 0)
		return DOCA_ERROR_INVALID_VALUE;

	result = get_dpu_state(objs, &state);
	if (result != DOCA_SUCCESS)
		return result;

#if DEBUG_LOG
	if (comp->ring_seq <= 8U || (comp->ring_seq % DEBUG_INTERVAL) == 0U) {
		DOCA_LOG_INFO("gRPC DMA completed: req=%u seq=%lu schema=%u len=%u expected_dma=%u",
			      comp->request_id, comp->ring_seq, comp->schema_id, comp->len,
			      comp->expected_dma);
	}
#endif

	if (comp->expected_dma > 1) {
		slot = (uint32_t)((comp->ring_seq - 1U) % DMESH_GRPC_MAX_PENDING);
		entry = &state->pending[slot];

		if (entry->ring_seq != comp->ring_seq) {
			entry->ring_seq = comp->ring_seq;
			entry->request_id = comp->request_id;
			entry->completed_dma = 0;
			entry->expected_dma = comp->expected_dma;
		}

		if (++entry->completed_dma < comp->expected_dma)
			return DOCA_SUCCESS;
	}

	return send_serialize_request(objs, comp);
}

void
dmesh_grpc_dpu_state_destroy(struct objects *objs)
{
	if (objs == NULL || objs->grpc_dpu_state == NULL)
		return;
	free(objs->grpc_dpu_state);
	objs->grpc_dpu_state = NULL;
}
