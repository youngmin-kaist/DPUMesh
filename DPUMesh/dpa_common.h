#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

#include <stddef.h>
#include <stdint.h>

#include "grpc/proto_meta.h"

#include <doca_mmap.h>

#define DMA_ADDR_ALIGN 128
#define DMA_ALIGN_UP(size)    (((size) + (DMA_ADDR_ALIGN - 1)) & ~(DMA_ADDR_ALIGN - 1))

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;
typedef uint64_t doca_dpa_dev_notification_completion_t;

#define DMESH_GRPC_SERIALIZER_THREADS 16U
#define DEBUG_INTERVAL (1024 * 128 + 7717)
// #define DEBUG_INTERVAL 0xffffffff
#define DEBUG_LOG 0

struct dpa_thread_arg {
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;
	doca_dpa_dev_buf_arr_t dpa_buf_arr;
	doca_dpa_dev_buf_arr_t dpa_consumer_state_buf_arr;
	uint32_t buf_arr_size;

	doca_dpa_dev_mmap_t host_mmap;
	doca_dpa_dev_mmap_t dpu_mmap;

	uint64_t src_addr;
	uint32_t buf_size;
	uint32_t pos;
	doca_dpa_dev_buf_arr_t dpa_host_mmap_buf_arr;
	doca_dpa_dev_buf_arr_t dpa_dpu_mmap_buf_arr;
	uint64_t host_base_addr;
	uint64_t dpu_base_addr;
	uint32_t host_buf_size;
	uint32_t thread_index;
	uint64_t pipeline_state;
	doca_dpa_dev_notification_completion_t main_notify;
	doca_dpa_dev_notification_completion_t serializer_notify[DMESH_GRPC_SERIALIZER_THREADS];
	uint32_t serializer_index;
	uint32_t reserved0;
} __attribute__((aligned(8)));

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
	COMCH_MSG_TYPE_GRPC_DMA_COMPLETED = 3,
	COMCH_MSG_TYPE_GRPC_SERIALIZE_REQ = 4,
	COMCH_MSG_TYPE_GRPC_SERIALIZE_COMPLETED = 5,
};

enum pipeline_task_state {
	TASK_STATE_IDLE = 0,
	TASK_STATE_PROCESSING = 1,
	TASK_STATE_COMPLETED = 2,
	TASK_STATE_ERROR = 3,
};

struct comch_dma_comp_msg {
	enum comch_msg_type type;
	uint32_t pos;
	uint32_t length;
	uint64_t idx;
};

typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;

#define DMA_RING_CACHELINE_SIZE 64
#define DMA_COMPLETION_BATCH_SIZE 64
#define DMA_MSG_SIZE_LIMIT 8192
#define DMA_COMPLETION_IDLE_POLL_LIMIT 1024

#define BUFFER_SIZE (1024 * 1024 * 32)

#define DMESH_GRPC_ARENA_ADDR_ALIGN 64U
#define DMESH_GRPC_DMA_MSG_ALIGN DMA_ADDR_ALIGN
#define DMESH_GRPC_MAX_FLAT_SIZE 8192U
#define DMESH_GRPC_MAX_ENCODED_SIZE 8192U
#define DMESH_GRPC_PRIVATE_SLOT_SIZE \
	DMA_ALIGN_UP(DMESH_GRPC_MAX_FLAT_SIZE + DMESH_GRPC_MAX_ENCODED_SIZE)
#define DMESH_GRPC_MAX_PENDING 1024U
#define DMESH_GRPC_SERIALIZER_QUEUE_DEPTH 16U
#define DMESH_GRPC_SERIALIZER_IDLE_POLL_BUDGET 1024U
#define DMESH_GRPC_SERIALIZER_DRR_QUANTUM DMESH_GRPC_MAX_FLAT_SIZE
#define DMESH_GRPC_SERIALIZER_DRR_MAX_DEFICIT \
	(DMESH_GRPC_SERIALIZER_DRR_QUANTUM * DMESH_GRPC_SERIALIZER_QUEUE_DEPTH)
#define DMESH_DPA_THREAD_MAIN 0U
#define DMESH_DPA_THREAD_MSG 1U
#define DMESH_DPA_THREAD_SERIALIZER_BASE 2U
#define DMESH_DPA_THREAD_COUNT \
	(DMESH_DPA_THREAD_SERIALIZER_BASE + DMESH_GRPC_SERIALIZER_THREADS)


struct dmesh_grpc_ref {
	uint32_t offset;
	uint32_t len;
};

struct dmesh_grpc_u32_array_ref {
	uint32_t offset;
	uint32_t count;
};

/*
 * Application-facing object. Pointer fields must point back into the exported
 * host mmap arena; DPA treats pointer values as host virtual DMA addresses.
 */
struct dmesh_grpc_hello_request {
	uint64_t id;
	const char *name;
	uint32_t name_len;
	const uint32_t *scores;
	uint32_t scores_count;
};

struct dmesh_grpc_hello_flat {
	uint64_t id;
	struct dmesh_grpc_ref name;
	struct dmesh_grpc_u32_array_ref scores;
};

struct comch_dma_req_msg {
	enum comch_msg_type type;
	uint32_t length;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_mmap_t src_mmap;
	doca_dpa_dev_mmap_t dst_mmap;
	uint64_t src_addr;
	uint64_t dst_addr;
} __attribute__((__packed__, aligned(8)));

struct comch_grpc_dma_comp_msg {
	enum comch_msg_type type;
	uint32_t request_id;
	uint64_t ring_seq;
	uint32_t len;
	uint16_t schema_id;
	uint16_t expected_dma;
} __attribute__((__packed__, aligned(8)));

struct comch_grpc_serialize_req_msg {
	enum comch_msg_type type;
	uint32_t request_id;
	uint64_t ring_seq;
	uint32_t len;
	uint16_t schema_id;
	uint8_t reserved[2];
} __attribute__((__packed__, aligned(8)));

struct comch_grpc_serialize_comp_msg {
	enum comch_msg_type type;
	uint32_t request_id;
	uint64_t ring_seq;
	uint32_t encoded_len;
	uint16_t schema_id;
	uint16_t completed;
} __attribute__((__packed__, aligned(8)));

struct dpa_grpc_serialize_task {
	uint32_t valid;
	uint32_t request_id;
	uint64_t ring_seq;
	uint32_t len;
	uint16_t schema_id;
} __attribute__((__packed__, aligned(8)));

struct dpa_grpc_pipeline_state {
	enum pipeline_task_state pipeline_task_state[DMESH_GRPC_MAX_PENDING];
	struct dpa_grpc_serialize_task
		serializer_tasks[DMESH_GRPC_SERIALIZER_THREADS][DMESH_GRPC_SERIALIZER_QUEUE_DEPTH];
	
	uint32_t serializer_drr_cursor;
	uint32_t reserved0;

	uint32_t serializer_prod[DMESH_GRPC_SERIALIZER_THREADS];
	uint32_t serializer_cons[DMESH_GRPC_SERIALIZER_THREADS];
	uint32_t serializer_drr_deficit[DMESH_GRPC_SERIALIZER_THREADS];
} __attribute__((aligned(4096)));

struct comch_msg {
	union
	{
		enum comch_msg_type type;
		// struct comch_dma_req_msg dma_req_msg;
		struct comch_dma_comp_msg dma_comp_msg;
		struct comch_grpc_dma_comp_msg grpc_dma_comp;
		struct comch_grpc_serialize_req_msg grpc_serialize_req;
		struct comch_grpc_serialize_comp_msg grpc_serialize_comp;
	};
} __attribute__((aligned(8)));

struct dma_desc {
	doca_dpa_dev_mmap_t mmap;
	uint32_t reserved0;
	uint64_t addr;
	uint64_t size;
	uint64_t seq;
	uint8_t reserved[32];
} __attribute__((__packed__, aligned(DMA_RING_CACHELINE_SIZE)));

struct grpc_req_desc {
	uint64_t addr;
	uint64_t seq;
	uint32_t size;
	uint32_t req_id;
	doca_dpa_dev_mmap_t mmap;
	uint16_t schema_id;
	uint8_t reserved[34];
} __attribute__((__packed__, aligned(DMA_RING_CACHELINE_SIZE)));

struct dma_ring_consumer_state {
	volatile uint64_t consumer_seq;
	uint8_t reserved[56];
} __attribute__((__packed__, aligned(DMA_RING_CACHELINE_SIZE)));

_Static_assert(sizeof(struct dma_desc) == DMA_RING_CACHELINE_SIZE,
	       "dma_desc must occupy exactly one cacheline");
_Static_assert(sizeof(struct grpc_req_desc) == DMA_RING_CACHELINE_SIZE,
	       "grpc_req_desc must occupy exactly one cacheline");
_Static_assert(sizeof(struct dma_ring_consumer_state) == DMA_RING_CACHELINE_SIZE,
	       "consumer state must occupy exactly one cacheline");

#endif
