#include "host_worker.h"
#include "object.h"
#include "comch_client.h"
#include "comch_producer.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "config.h"
#include "buffer.h"
#include "comch_msgq.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "ring.h"
#include "grpc/grpc_offload.h"

#include <doca_log.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

DOCA_LOG_REGISTER(HOST_WORKER_BENCH);

#ifndef HOST_WORKER_BENCH_LOG_INTERVAL
#define HOST_WORKER_BENCH_LOG_INTERVAL 65536U
#endif

#ifndef CLOCK_THREAD_CPUTIME_ID
#define CLOCK_THREAD_CPUTIME_ID CLOCK_MONOTONIC
#endif

struct host_worker_bench_stats {
	uint64_t submitted_requests;
	uint64_t alloc_success;
	uint64_t alloc_no_memory;
	uint64_t submit_errors;

	uint64_t pe_progress_calls;
	uint64_t pe_progress_positive_calls;
	uint64_t pe_progress_zero_calls;
	uint64_t pe_progress_events;

	uint64_t reclaim_calls;
	uint64_t reclaim_progress_calls;
	uint64_t reclaim_idle_calls;

	uint64_t wait_free_slot_iters;
	uint64_t wait_free_slot_progress_iters;
	uint64_t wait_free_slot_idle_iters;
	uint64_t wait_free_slot_pe_progress_events;

	uint64_t no_memory_retry_calls;
	uint64_t no_memory_retry_progress_calls;
	uint64_t no_memory_retry_idle_calls;

	uint64_t active_ns;
	uint64_t poll_ns;

	uint64_t alloc_active_ns;
	uint64_t alloc_no_memory_ns;
	uint64_t submit_publish_ns;
	uint64_t pe_progress_active_ns;
	uint64_t pe_progress_poll_ns;
	uint64_t reclaim_active_ns;
	uint64_t reclaim_poll_ns;
	uint64_t wait_free_slot_active_ns;
	uint64_t wait_free_slot_poll_ns;
	uint64_t no_memory_retry_active_ns;
	uint64_t no_memory_retry_poll_ns;
};

static inline uint64_t
bench_ns_from_timespec(const struct timespec *ts)
{
	return ((uint64_t)ts->tv_sec * UINT64_C(1000000000)) + (uint64_t)ts->tv_nsec;
}

static inline uint64_t
bench_thread_cpu_ns(void)
{
	struct timespec ts;

	(void)clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
	return bench_ns_from_timespec(&ts);
}

static inline uint64_t
bench_wall_ns(void)
{
	struct timespec ts;

	(void)clock_gettime(CLOCK_MONOTONIC, &ts);
	return bench_ns_from_timespec(&ts);
}

static inline void
bench_add_active(struct host_worker_bench_stats *stats, uint64_t *bucket, uint64_t start_ns)
{
	uint64_t delta = bench_thread_cpu_ns() - start_ns;

	stats->active_ns += delta;
	*bucket += delta;
}

static inline void
bench_add_poll(struct host_worker_bench_stats *stats, uint64_t *bucket, uint64_t start_ns)
{
	uint64_t delta = bench_thread_cpu_ns() - start_ns;

	stats->poll_ns += delta;
	*bucket += delta;
}

static inline uint64_t
bench_bp(uint64_t num, uint64_t den)
{
	if (den == 0)
		return 0;
	return (num * UINT64_C(10000)) / den;
}

static void
bench_stats_delta(struct host_worker_bench_stats *delta,
		  const struct host_worker_bench_stats *cur,
		  const struct host_worker_bench_stats *last)
{
	const uint64_t *cur_words = (const uint64_t *)cur;
	const uint64_t *last_words = (const uint64_t *)last;
	uint64_t *delta_words = (uint64_t *)delta;
	size_t words = sizeof(*cur) / sizeof(uint64_t);
	size_t i;

	_Static_assert(sizeof(*cur) % sizeof(uint64_t) == 0,
		       "host_worker_bench_stats must stay uint64_t-only");
	for (i = 0; i < words; ++i)
		delta_words[i] = cur_words[i] - last_words[i];
}

static void
bench_report(const struct host_worker_bench_stats *cur,
	     struct host_worker_bench_stats *last,
	     uint64_t *last_wall_ns)
{
	struct host_worker_bench_stats delta;
	uint64_t now_wall_ns = bench_wall_ns();
	uint64_t wall_delta_ns = now_wall_ns - *last_wall_ns;
	uint64_t classified_cpu_ns;
	uint64_t active_share_bp;
	uint64_t active_wall_bp;
	uint64_t poll_wall_bp;
	uint64_t active_per_req;
	uint64_t poll_per_req;

	bench_stats_delta(&delta, cur, last);
	classified_cpu_ns = delta.active_ns + delta.poll_ns;
	active_share_bp = bench_bp(delta.active_ns, classified_cpu_ns);
	active_wall_bp = bench_bp(delta.active_ns, wall_delta_ns);
	poll_wall_bp = bench_bp(delta.poll_ns, wall_delta_ns);
	active_per_req = delta.submitted_requests == 0 ? 0 :
			 delta.active_ns / delta.submitted_requests;
	poll_per_req = delta.submitted_requests == 0 ? 0 :
		       delta.poll_ns / delta.submitted_requests;

	DOCA_LOG_INFO("host worker bench interval: req=%" PRIu64 " total=%" PRIu64
		      " wall_ms=%" PRIu64 " active_ms=%" PRIu64 " poll_ms=%" PRIu64
		      " active_share=%" PRIu64 ".%02" PRIu64 "%%"
		      " active_wall=%" PRIu64 ".%02" PRIu64 "%%"
		      " poll_wall=%" PRIu64 ".%02" PRIu64 "%%"
		      " active_ns_per_req=%" PRIu64 " poll_ns_per_req=%" PRIu64,
		      delta.submitted_requests, cur->submitted_requests,
		      wall_delta_ns / UINT64_C(1000000),
		      delta.active_ns / UINT64_C(1000000),
		      delta.poll_ns / UINT64_C(1000000),
		      active_share_bp / 100, active_share_bp % 100,
		      active_wall_bp / 100, active_wall_bp % 100,
		      poll_wall_bp / 100, poll_wall_bp % 100,
		      active_per_req, poll_per_req);

	DOCA_LOG_INFO("host worker bench detail: alloc_ns=%" PRIu64
		      " publish_ns=%" PRIu64
		      " reclaim_active_ns=%" PRIu64 " reclaim_poll_ns=%" PRIu64
		      " pe_active_ns=%" PRIu64 " pe_poll_ns=%" PRIu64
		      " wait_active_ns=%" PRIu64 " wait_poll_ns=%" PRIu64
		      " nomem_alloc_ns=%" PRIu64 " nomem_retry_active_ns=%" PRIu64
		      " nomem_retry_poll_ns=%" PRIu64,
		      delta.alloc_active_ns,
		      delta.submit_publish_ns,
		      delta.reclaim_active_ns, delta.reclaim_poll_ns,
		      delta.pe_progress_active_ns, delta.pe_progress_poll_ns,
		      delta.wait_free_slot_active_ns, delta.wait_free_slot_poll_ns,
		      delta.alloc_no_memory_ns, delta.no_memory_retry_active_ns,
		      delta.no_memory_retry_poll_ns);

	DOCA_LOG_INFO("host worker bench counts: pe_calls=%" PRIu64
		      " pe_positive=%" PRIu64 " pe_zero=%" PRIu64
		      " pe_events=%" PRIu64 " reclaim_progress=%" PRIu64
		      " reclaim_idle=%" PRIu64 " wait_iters=%" PRIu64
		      " wait_progress=%" PRIu64 " wait_idle=%" PRIu64
		      " wait_pe_events=%" PRIu64 " nomem=%" PRIu64
		      " nomem_retry_progress=%" PRIu64 " nomem_retry_idle=%" PRIu64
		      " submit_errors=%" PRIu64,
		      delta.pe_progress_calls,
		      delta.pe_progress_positive_calls,
		      delta.pe_progress_zero_calls,
		      delta.pe_progress_events,
		      delta.reclaim_progress_calls,
		      delta.reclaim_idle_calls,
		      delta.wait_free_slot_iters,
		      delta.wait_free_slot_progress_iters,
		      delta.wait_free_slot_idle_iters,
		      delta.wait_free_slot_pe_progress_events,
		      delta.alloc_no_memory,
		      delta.no_memory_retry_progress_calls,
		      delta.no_memory_retry_idle_calls,
		      delta.submit_errors);

	*last = *cur;
	*last_wall_ns = now_wall_ns;
}

static inline uint64_t
random_chars_next(uint64_t *state)
{
	uint64_t x = *state;

	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	*state = x;

	return x * UINT64_C(2685821657736338717);
}

static inline uint64_t
random_chars_seed(void)
{
	uint64_t seed = (uint64_t)time(NULL);

	seed ^= (uint64_t)clock() << 32;
	seed ^= (uint64_t)(uintptr_t)&seed;

	if (seed == 0)
		seed = UINT64_C(0x9e3779b97f4a7c15);

	return seed;
}

static inline void
fill_random_chars(char *buf, size_t len)
{
	static _Thread_local uint64_t state;
	size_t offset = 0;

	if (buf == NULL || len == 0)
		return;

	if (state == 0)
		state = random_chars_seed();

	while (len - offset >= sizeof(uint64_t)) {
		uint64_t value = random_chars_next(&state);

		memcpy(buf + offset, &value, sizeof(value));
		offset += sizeof(value);
	}

	if (offset < len) {
		uint64_t value = random_chars_next(&state);

		memcpy(buf + offset, &value, len - offset);
	}
}

static inline void
fill_random_numbers(uint32_t *buf, int count)
{
	static _Thread_local uint64_t state;
	int i = 0;

	if (buf == NULL || count <= 0)
		return;

	if (state == 0)
		state = random_chars_seed();

	while (i + 1 < count) {
		uint64_t value = random_chars_next(&state);

		buf[i++] = (uint32_t)value;
		buf[i++] = (uint32_t)(value >> 32);
	}

	if (i < count)
		buf[i] = (uint32_t)random_chars_next(&state);
}

static int
bench_progress_pe_once(struct doca_pe *pe, struct host_worker_bench_stats *stats)
{
	uint64_t start_ns;
	int progress;

	if (pe == NULL)
		return 0;

	start_ns = bench_thread_cpu_ns();
	progress = doca_pe_progress(pe);
	stats->pe_progress_calls++;
	if (progress > 0) {
		stats->pe_progress_positive_calls++;
		stats->pe_progress_events += (uint64_t)progress;
		bench_add_active(stats, &stats->pe_progress_active_ns, start_ns);
	} else {
		stats->pe_progress_zero_calls++;
		bench_add_poll(stats, &stats->pe_progress_poll_ns, start_ns);
	}

	return progress;
}

static void
bench_drain_pe(struct doca_pe *pe, struct host_worker_bench_stats *stats)
{
	while (bench_progress_pe_once(pe, stats) > 0) {
		/* Drain until the first zero-progress call, which is counted as polling. */
	}
}

static void
bench_refresh_reclaim(struct objects *objs,
		      struct dmesh_grpc_arena *arena,
		      struct host_worker_bench_stats *stats)
{
	struct dma_ring *ring = objs->dma_ring;
	uint64_t before_seq = ring->observed_consumer_seq;
	uint64_t start_ns = bench_thread_cpu_ns();
	uint64_t after_seq;

	dma_ring_refresh_consumer(ring);
	after_seq = ring->observed_consumer_seq;
	dmesh_grpc_arena_reclaim_through(arena, after_seq);

	stats->reclaim_calls++;
	/*
	 * Classification assumption: a consumer_seq change means DPA made
	 * observable progress and the reclaim scan is useful host work. An
	 * unchanged consumer_seq is counted as polling overhead. This does not
	 * prove that a block was released; it separates host-observable progress
	 * from empty shared-memory polling.
	 */
	if (after_seq != before_seq) {
		stats->reclaim_progress_calls++;
		bench_add_active(stats, &stats->reclaim_active_ns, start_ns);
	} else {
		stats->reclaim_idle_calls++;
		bench_add_poll(stats, &stats->reclaim_poll_ns, start_ns);
	}
}

static void
bench_no_memory_retry(struct objects *objs,
		      struct dmesh_grpc_arena *arena,
		      struct host_worker_bench_stats *stats)
{
	struct dma_ring *ring = objs->dma_ring;
	uint64_t before_seq = ring->observed_consumer_seq;
	uint64_t start_ns = bench_thread_cpu_ns();
	uint64_t after_seq;
	int progress;

	dma_ring_refresh_consumer(ring);
	after_seq = ring->observed_consumer_seq;
	dmesh_grpc_arena_reclaim_through(arena, after_seq);
	progress = objs->producer_pe == NULL ? 0 : doca_pe_progress(objs->producer_pe);

	stats->no_memory_retry_calls++;
	if (after_seq != before_seq || progress > 0) {
		stats->no_memory_retry_progress_calls++;
		bench_add_active(stats, &stats->no_memory_retry_active_ns, start_ns);
	} else {
		stats->no_memory_retry_idle_calls++;
		bench_add_poll(stats, &stats->no_memory_retry_poll_ns, start_ns);
	}
}

static bool
ptr_in_dma_buffer(const struct objects *objs, const void *ptr, size_t size)
{
	uintptr_t base;
	uintptr_t cur;
	size_t offset;

	if (objs == NULL || objs->dma_buffer == NULL || ptr == NULL)
		return false;
	if (size > BUFFER_SIZE)
		return false;

	base = (uintptr_t)objs->dma_buffer;
	cur = (uintptr_t)ptr;
	if (cur < base)
		return false;

	offset = (size_t)(cur - base);
	return offset <= (BUFFER_SIZE - size);
}

static void
bench_wait_for_free_slot_once(struct objects *objs,
			      struct dmesh_grpc_arena *arena,
			      struct host_worker_bench_stats *stats)
{
	struct dma_ring *ring = objs->dma_ring;
	uint64_t before_seq = ring->observed_consumer_seq;
	uint64_t start_ns = bench_thread_cpu_ns();
	uint64_t after_seq;
	int progress;

	dma_ring_refresh_consumer(ring);
	after_seq = ring->observed_consumer_seq;
	dmesh_grpc_arena_reclaim_through(arena, after_seq);
	progress = objs->producer_pe == NULL ? 0 : doca_pe_progress(objs->producer_pe);

	stats->wait_free_slot_iters++;
	if (progress > 0)
		stats->wait_free_slot_pe_progress_events += (uint64_t)progress;

	if (after_seq != before_seq || progress > 0) {
		stats->wait_free_slot_progress_iters++;
		bench_add_active(stats, &stats->wait_free_slot_active_ns, start_ns);
	} else {
		stats->wait_free_slot_idle_iters++;
		bench_add_poll(stats, &stats->wait_free_slot_poll_ns, start_ns);
	}
}

static doca_error_t
bench_submit_request(struct objects *objs,
		     uint32_t schema_id,
		     struct dmesh_grpc_arena *arena,
		     const void *rpc_obj,
		     size_t rpc_obj_size,
		     uint32_t request_id,
		     struct host_worker_bench_stats *stats)
{
	struct grpc_req_desc *desc;
	struct dma_ring *ring;
	uint64_t producer_seq;
	doca_dpa_dev_mmap_t local_mmap;
	doca_error_t result;
	uint64_t start_ns;

	if (objs == NULL || objs->dma_ring == NULL || objs->local_mmap == NULL ||
	    arena == NULL || rpc_obj == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if (!ptr_in_dma_buffer(objs, rpc_obj, rpc_obj_size)) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		return DOCA_ERROR_INVALID_VALUE;
	}

	ring = objs->dma_ring;
	while (!dma_ring_has_free_slot(ring))
		bench_wait_for_free_slot_once(objs, arena, stats);

	start_ns = bench_thread_cpu_ns();
	result = doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &local_mmap);
	if (result != DOCA_SUCCESS) {
		dmesh_grpc_arena_abort_request(arena, rpc_obj);
		bench_add_active(stats, &stats->submit_publish_ns, start_ns);
		return result;
	}

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
		bench_add_active(stats, &stats->submit_publish_ns, start_ns);
		return result;
	}

	/*
	 * Descriptor ownership and ordering match dmesh_grpc_submit_request().
	 * Host publishes descriptor fields before seq/producer_seq; if the
	 * platform needs an explicit host-to-PCI write barrier, add it in both
	 * the production helper and this bench helper.
	 */
	desc->seq = producer_seq + 1U;
	ring->producer_seq = producer_seq + 1U;
	bench_add_active(stats, &stats->submit_publish_ns, start_ns);
	return DOCA_SUCCESS;
}

void
run_host_worker(struct objects *objs)
{
	doca_error_t result;
	struct dmesh_grpc_arena app_arena = {0};
	struct host_worker_bench_stats stats = {0};
	struct host_worker_bench_stats last_stats = {0};
	uint64_t last_report_wall_ns = bench_wall_ns();
	uint32_t request_id = 1;
	int pe_progress;
	char name[8192];
	int name_len = 8000;
	uint32_t *scores = NULL;
	int scores_count = 0;

	DOCA_LOG_INFO("Starting Host worker bench");

	result = init_comch_ctrl_path_client("DPUMesh", objs, true);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to init comch control path client: %s", doca_error_get_descr(result));
		cleanup_objects(objs);
		goto argp_cleanup;
	}

	result = init_comch_datapath_producer(objs);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send message over comch data path: %s", doca_error_get_descr(result));
		cleanup_objects(objs);
		goto argp_cleanup;
	}

	result = setup_dma_ring(objs, DMA_RING_SIZE);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to setup DMA ring: %s", doca_error_get_descr(result));
		goto argp_cleanup;
	}

	result = alloc_hugepage_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
						    &objs->dma_buffer, BUFFER_SIZE,
						    DOCA_ACCESS_FLAG_PCI_READ_WRITE);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
		goto argp_cleanup;
	}

	result = export_mmap_to_remote(objs, objs->local_mmap,
				       objs->dma_buffer, BUFFER_SIZE,
				       DMA_BUFFER, HOST_TO_DPU);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
		goto argp_cleanup;
	}

	DOCA_LOG_INFO("consumer state magic: 0x%" PRIx64,
		      (uint64_t)objs->dma_ring->consumer_state->consumer_seq);
	result = dmesh_grpc_arena_init(&app_arena, objs->dma_buffer, BUFFER_SIZE);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to initialize gRPC arena: %s", doca_error_get_descr(result));
		goto argp_cleanup;
	}
	objs->grpc_offload = &app_arena;

	while ((pe_progress = doca_pe_progress(objs->pe)) > 0)
		DOCA_LOG_INFO("Made progress on PE: %d", pe_progress);

	fill_random_chars(name, sizeof(name));
	if (scores_count > 0) {
		scores = malloc((size_t)scores_count * sizeof(uint32_t));
		if (scores == NULL) {
			DOCA_LOG_ERR("Failed to allocate scores buffer");
			goto argp_cleanup;
		}
		fill_random_numbers(scores, scores_count);
	}

	while (true) {
		struct dmesh_grpc_hello_flat *flat = NULL;
		size_t flat_len = 0;
		uint64_t start_ns;

		bench_drain_pe(objs->pe, &stats);
		bench_refresh_reclaim(objs, &app_arena, &stats);

		start_ns = bench_thread_cpu_ns();
		result = dmesh_grpc_hello_flat_alloc(&app_arena,
						     request_id,
						     name,
						     (uint32_t)name_len,
						     scores,
						     (uint32_t)scores_count,
						     &flat,
						     &flat_len);
		if (result == DOCA_ERROR_NO_MEMORY) {
			stats.alloc_no_memory++;
			bench_add_poll(&stats, &stats.alloc_no_memory_ns, start_ns);
			bench_no_memory_retry(objs, &app_arena, &stats);
			continue;
		}
		if (result != DOCA_SUCCESS)
			goto argp_cleanup;

		stats.alloc_success++;
		bench_add_active(&stats, &stats.alloc_active_ns, start_ns);

		result = bench_submit_request(objs,
					      DMESH_GRPC_SCHEMA_HELLO_REQUEST,
					      &app_arena,
					      flat,
					      flat_len,
					      request_id,
					      &stats);
		if (result != DOCA_SUCCESS) {
			stats.submit_errors++;
			DOCA_LOG_ERR("Failed to submit gRPC request descriptor: %s",
				     doca_error_get_descr(result));
			goto argp_cleanup;
		}

		stats.submitted_requests++;
		request_id++;

		if ((stats.submitted_requests % HOST_WORKER_BENCH_LOG_INTERVAL) == 0)
			bench_report(&stats, &last_stats, &last_report_wall_ns);
	}

	DOCA_LOG_INFO("Finished Host worker bench");

argp_cleanup:
	if (objs != NULL && objs->grpc_offload == &app_arena)
		objs->grpc_offload = NULL;
	free(scores);
	dmesh_grpc_arena_destroy(&app_arena);
	if (objs != NULL) {
		destroy_mmap_and_unmap_hugepage_buffer(objs->local_mmap, objs->dma_buffer, BUFFER_SIZE);
		objs->local_mmap = NULL;
		objs->dma_buffer = NULL;
	}
	clean_argp();
	return;
}
