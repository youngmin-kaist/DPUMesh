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

#define DMESH_GRPC_ARENA_NO_BLOCK UINT32_MAX

struct dmesh_grpc_pending_entry {
	bool valid;
	uint32_t request_id;
	uint32_t schema_id;
	uint32_t expected_dma;
	uint32_t completed_dma;
	uint64_t ring_seq;
	uint64_t flat_addr;
	uint32_t flat_len;
	uint64_t out_addr;
	uint32_t out_cap;
};

struct dmesh_grpc_dpu_state {
	struct dmesh_grpc_pending_entry pending[DMESH_GRPC_MAX_PENDING];
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
dmesh_grpc_arena_release_block(struct dmesh_grpc_arena *arena, uint32_t block_idx)
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
	block->offset = 0;
	block->owner_ring_seq = 0;
	block->in_use = 1;
	block->active = 0;
	block->next_free = DMESH_GRPC_ARENA_NO_BLOCK;
	return block_idx;
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
	arena->capacity = capacity;
	arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
	arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
	arena->blocks = calloc(arena->block_count, sizeof(*arena->blocks));
	if (arena->blocks == NULL)
		return DOCA_ERROR_NO_MEMORY;

	for (i = 0; i < arena->block_count; ++i)
		dmesh_grpc_arena_release_block(arena, arena->block_count - 1U - i);
	return DOCA_SUCCESS;
}

void
dmesh_grpc_arena_destroy(struct dmesh_grpc_arena *arena)
{
	if (arena == NULL)
		return;
	free(arena->blocks);
	memset(arena, 0, sizeof(*arena));
	arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
	arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
}

void
dmesh_grpc_arena_reset(struct dmesh_grpc_arena *arena)
{
	uint32_t i;

	if (arena == NULL)
		return;
	arena->free_head = DMESH_GRPC_ARENA_NO_BLOCK;
	arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
	for (i = 0; i < arena->block_count; ++i)
		dmesh_grpc_arena_release_block(arena, arena->block_count - 1U - i);
}

size_t
dmesh_grpc_arena_available(const struct dmesh_grpc_arena *arena)
{
	size_t count = 0;
	uint32_t block_idx;

	if (arena == NULL || arena->blocks == NULL)
		return 0;
	for (block_idx = arena->free_head;
	     block_idx != DMESH_GRPC_ARENA_NO_BLOCK && block_idx < arena->block_count;
	     block_idx = arena->blocks[block_idx].next_free)
		count++;
	return count * arena->block_size;
}

void *
dmesh_grpc_arena_alloc(struct dmesh_grpc_arena *arena, size_t size, size_t align)
{
	size_t off;
	size_t alloc_size;
	size_t block_base_off;
	uintptr_t base_addr;
	uintptr_t raw_addr;
	uintptr_t aligned_addr;
	struct dmesh_grpc_arena_block *block;

	if (arena == NULL || arena->base == NULL || arena->blocks == NULL)
		return NULL;
	if (align < DMESH_GRPC_ARENA_ADDR_ALIGN)
		align = DMESH_GRPC_ARENA_ADDR_ALIGN;
	if (arena->current_block == DMESH_GRPC_ARENA_NO_BLOCK) {
		arena->current_block = dmesh_grpc_arena_pop_block(arena);
		if (arena->current_block == DMESH_GRPC_ARENA_NO_BLOCK)
			return NULL;
	}
	if (arena->current_block >= arena->block_count)
		return NULL;

	/*
	 * Pointer fields are DMA sources. Keep every returned address 64B
	 * aligned and reserve zero-filled 128B chunks so DPA can round DMA copy
	 * sizes without reading another object's contents.
	 */
	block = &arena->blocks[arena->current_block];
	block_base_off = (size_t)arena->current_block * arena->block_size;
	base_addr = (uintptr_t)(arena->base + block_base_off);
	if (block->offset > UINTPTR_MAX - base_addr)
		return NULL;
	raw_addr = base_addr + block->offset;
	aligned_addr = align_up_uintptr(raw_addr, align);
	if (aligned_addr == UINTPTR_MAX || aligned_addr < base_addr)
		return NULL;
	off = (size_t)(aligned_addr - base_addr);
	alloc_size = align_up_size(size, DMESH_GRPC_DMA_MSG_ALIGN);
	if (off == SIZE_MAX || alloc_size == SIZE_MAX ||
	    off > arena->block_size || alloc_size > arena->block_size - off)
		return NULL;

	block->offset = off + alloc_size;
	memset((void *)aligned_addr, 0, alloc_size);
	return (void *)aligned_addr;
}

void
dmesh_grpc_arena_abort_request(struct dmesh_grpc_arena *arena, const void *request)
{
	uint32_t block_idx;

	block_idx = dmesh_grpc_arena_block_for_ptr(arena, request);
	if (block_idx == DMESH_GRPC_ARENA_NO_BLOCK)
		return;
	if (arena->blocks[block_idx].active != 0)
		return;
	if (arena->current_block == block_idx)
		arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
	dmesh_grpc_arena_release_block(arena, block_idx);
}

doca_error_t
dmesh_grpc_arena_finish_request(struct dmesh_grpc_arena *arena,
				const void *request,
				uint64_t ring_seq)
{
	uint32_t block_idx;
	struct dmesh_grpc_arena_block *block;

	if (ring_seq == 0)
		return DOCA_ERROR_INVALID_VALUE;
	block_idx = dmesh_grpc_arena_block_for_ptr(arena, request);
	if (block_idx == DMESH_GRPC_ARENA_NO_BLOCK)
		return DOCA_ERROR_INVALID_VALUE;

	block = &arena->blocks[block_idx];
	if (block->in_use == 0 || block->active != 0)
		return DOCA_ERROR_BAD_STATE;

	/*
	 * Ownership assumption: DPA may keep DMA reads against this host block
	 * until it reports GRPC_DMA_COMPLETED for ring_seq. Host must not recycle
	 * the block before that completion path calls reclaim_ring_seq().
	 */
	block->owner_ring_seq = ring_seq;
	block->active = 1;
	if (arena->current_block == block_idx)
		arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
	return DOCA_SUCCESS;
}

void
dmesh_grpc_arena_reclaim_ring_seq(struct dmesh_grpc_arena *arena, uint64_t ring_seq)
{
	uint32_t i;

	if (arena == NULL || arena->blocks == NULL || ring_seq == 0)
		return;

	for (i = 0; i < arena->block_count; ++i) {
		if (arena->blocks[i].active != 0 &&
		    arena->blocks[i].owner_ring_seq == ring_seq) {
			dmesh_grpc_arena_release_block(arena, i);
			return;
		}
	}
}

void
dmesh_grpc_arena_reclaim_through(struct dmesh_grpc_arena *arena, uint64_t consumer_seq)
{
	uint32_t i;

	if (arena == NULL || arena->blocks == NULL || consumer_seq == 0)
		return;

	/*
	 * consumer_seq is published by DPA only after the serialize completion
	 * message is posted and the descriptor slot is released. This assumes the
	 * host observes that PCI write before reclaiming owner_ring_seq blocks.
	 */
	for (i = 0; i < arena->block_count; ++i) {
		if (arena->blocks[i].active != 0 &&
		    arena->blocks[i].owner_ring_seq <= consumer_seq)
			dmesh_grpc_arena_release_block(arena, i);
	}
}

doca_error_t
dmesh_grpc_hello_request_alloc(struct dmesh_grpc_arena *arena,
			       uint64_t id,
			       const char *name,
			       uint32_t name_len,
			       const uint32_t *scores,
			       uint32_t scores_count,
			       struct dmesh_grpc_hello_request **out)
{
	struct dmesh_grpc_hello_request *req;
	uint32_t saved_block;

	if (arena == NULL || out == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if ((name_len != 0 && name == NULL) || (scores_count != 0 && scores == NULL))
		return DOCA_ERROR_INVALID_VALUE;

	*out = NULL;
	saved_block = arena->current_block;
	req = dmesh_grpc_arena_alloc(arena, sizeof(*req), DMESH_GRPC_ARENA_ADDR_ALIGN);
	if (req == NULL)
		return DOCA_ERROR_NO_MEMORY;
	if (saved_block == DMESH_GRPC_ARENA_NO_BLOCK)
		saved_block = arena->current_block;

	req->id = id;

	if (name_len != 0) {
		char *name_dst = dmesh_grpc_arena_alloc(arena, name_len,
							DMESH_GRPC_ARENA_ADDR_ALIGN);
		if (name_dst == NULL) {
			if (saved_block < arena->block_count)
				dmesh_grpc_arena_release_block(arena, saved_block);
			arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
			return DOCA_ERROR_NO_MEMORY;
		}
		memcpy(name_dst, name, name_len);
		req->name = name_dst;
		req->name_len = name_len;
	}

	if (scores_count != 0) {
		size_t scores_bytes = (size_t)scores_count * sizeof(uint32_t);
		uint32_t *scores_dst = dmesh_grpc_arena_alloc(arena, scores_bytes,
							      DMESH_GRPC_ARENA_ADDR_ALIGN);
		if (scores_dst == NULL) {
			if (saved_block < arena->block_count)
				dmesh_grpc_arena_release_block(arena, saved_block);
			arena->current_block = DMESH_GRPC_ARENA_NO_BLOCK;
			return DOCA_ERROR_NO_MEMORY;
		}
		memcpy(scores_dst, scores, scores_bytes);
		req->scores = scores_dst;
		req->scores_count = scores_count;
	}

	*out = req;
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
	uint64_t producer_seq;
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
		dmesh_grpc_arena_reclaim_through(arena, ring->observed_consumer_seq);
		(void)doca_pe_progress(objs->producer_pe);
	}

	result = doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &local_mmap);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		return result;
	}


	// get new request descriptor and fill it out
	producer_seq = ring->producer_seq;
	desc = get_grpc_req_desc_for_seq(ring, producer_seq);
	memset(desc, 0, sizeof(*desc));
	desc->mmap = local_mmap;
	desc->addr = (uint64_t)(uintptr_t)rpc_obj;
	desc->size = rpc_obj_size;
	desc->schema_id = schema_id;
	desc->req_id = request_id;

	result = dmesh_grpc_arena_finish_request(arena, rpc_obj, producer_seq + 1U);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		return result;
	}

	/*
	 * Host owns descriptor writes until seq is published. DPA polls seq with
	 * acquire semantics and may DMA pointer fields after reading this object.
	 */
	// __atomic_store_n(&desc->seq, producer_seq + 1U, __ATOMIC_RELEASE);
	desc->seq = producer_seq + 1U;
	ring->producer_seq = producer_seq + 1U;
	return DOCA_SUCCESS;
}

static doca_error_t
send_serialize_request(struct objects *objs, const struct dmesh_grpc_pending_entry *entry)
{
	struct comch_grpc_serialize_req_msg msg;

	if (objs == NULL || objs->dpa_comch == NULL || entry == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	memset(&msg, 0, sizeof(msg));
	msg.type = COMCH_MSG_TYPE_GRPC_SERIALIZE_REQ;
	msg.request_id = entry->request_id;
	msg.schema_id = entry->schema_id;
	msg.ring_seq = entry->ring_seq;
	msg.flat_addr = entry->flat_addr;
	msg.flat_len = entry->flat_len;
	msg.out_addr = entry->out_addr;
	msg.out_cap = entry->out_cap;

	if (entry->ring_seq <= 8U || (entry->ring_seq % DEBUG_INTERVAL) == 0U) {
		DOCA_LOG_INFO("gRPC sending serialize req: req=%u seq=%lu schema=%u flat=0x%lx len=%u out=0x%lx cap=%u",
			      entry->request_id, entry->ring_seq, entry->schema_id,
			      entry->flat_addr, entry->flat_len,
			      entry->out_addr, entry->out_cap);
	}

	return dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send, &msg, sizeof(msg));
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
				 const struct comch_grpc_dma_comp_msg *comp)
{
	struct dmesh_grpc_dpu_state *state;
	struct dmesh_grpc_pending_entry *entry;
	uint32_t slot;
	doca_error_t result;

	if (objs == NULL || comp == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if (comp->ring_seq == 0)
		return DOCA_ERROR_INVALID_VALUE;
	if (comp->status != 0) {
		DOCA_LOG_ERR("gRPC DMA preparation failed: request_id=%u status=%u",
			     comp->request_id, comp->status);
		dmesh_grpc_arena_reclaim_ring_seq((struct dmesh_grpc_arena *)objs->grpc_offload,
						  comp->ring_seq);
		return DOCA_ERROR_BAD_STATE;
	}

	result = get_dpu_state(objs, &state);
	if (result != DOCA_SUCCESS)
		return result;

	slot = (uint32_t)((comp->ring_seq - 1U) % DMESH_GRPC_MAX_PENDING);
	entry = &state->pending[slot];
	if (!entry->valid) {
		memset(entry, 0, sizeof(*entry));
		entry->valid = true;
		entry->request_id = comp->request_id;
		entry->schema_id = comp->schema_id;
		entry->expected_dma = comp->expected_dma;
		entry->ring_seq = comp->ring_seq;
		entry->flat_addr = comp->flat_addr;
		entry->flat_len = comp->flat_len;
		entry->out_addr = comp->out_addr;
		entry->out_cap = comp->out_cap;
	} else if (entry->ring_seq != comp->ring_seq) {
		/*
		 * The pending table is ring_seq modulo a fixed size. This assumes
		 * DPA never has more than DMESH_GRPC_MAX_PENDING prepared descriptors
		 * whose serialization has not completed.
		 */
		DOCA_LOG_ERR("gRPC pending slot collision: slot=%u old_seq=%lu new_seq=%lu",
			     slot, entry->ring_seq, comp->ring_seq);
		return DOCA_ERROR_AGAIN;
	}

	if (entry->expected_dma > 0) {
		entry->completed_dma++;
		if (comp->ring_seq <= 8U || (comp->ring_seq % DEBUG_INTERVAL) == 0U) {
			DOCA_LOG_INFO("gRPC DMA completed: req=%u seq=%lu completed=%u/%u flat=0x%lx out=0x%lx",
				      comp->request_id, comp->ring_seq,
				      entry->completed_dma, entry->expected_dma,
				      entry->flat_addr, entry->out_addr);
		}
		if (entry->completed_dma < entry->expected_dma)
			return DOCA_SUCCESS;
	} else if (comp->ring_seq <= 8U || (comp->ring_seq % DEBUG_INTERVAL) == 0U) {
		DOCA_LOG_INFO("gRPC DMA completed: req=%u seq=%lu completed=0/0 flat=0x%lx out=0x%lx",
			      comp->request_id, comp->ring_seq,
			      entry->flat_addr, entry->out_addr);
	}

	result = send_serialize_request(objs, entry);
	entry->valid = false;

	dmesh_grpc_arena_reclaim_ring_seq((struct dmesh_grpc_arena *)objs->grpc_offload,
					  comp->ring_seq);
	return result;
}

void
dmesh_grpc_dpu_state_destroy(struct objects *objs)
{
	if (objs == NULL || objs->grpc_dpu_state == NULL)
		return;
	free(objs->grpc_dpu_state);
	objs->grpc_dpu_state = NULL;
}
