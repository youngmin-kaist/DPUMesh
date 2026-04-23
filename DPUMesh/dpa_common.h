#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

#include <stddef.h>
#include <stdint.h>
#include <doca_mmap.h>

#define DMA_ADDR_ALIGN 128

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

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

} __attribute__((__packed__, aligned(8)));

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
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
#define DMA_COMPLETION_BATCH_SIZE 256
#define DMA_COMPLETION_IDLE_POLL_LIMIT 1024

struct comch_dma_req_msg {
	enum comch_msg_type type;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_mmap_t src_mmap;
	doca_dpa_dev_mmap_t dst_mmap;
	uint64_t src_addr;
	uint64_t dst_addr;
	uint32_t length;
} __attribute__((__packed__, aligned(8)));

struct comch_msg {
	enum comch_msg_type type;
	union
	{
		struct comch_dma_req_msg dma_req_msg;
		struct comch_dma_comp_msg dma_comp_msg;
	};
} __attribute__((__packed__, aligned(4)));

struct dma_desc {
	doca_dpa_dev_mmap_t mmap;
	uint32_t reserved0;
	uint64_t addr;
	uint64_t size;
	uint64_t seq;
	uint8_t reserved[32];
} __attribute__((__packed__, aligned(DMA_RING_CACHELINE_SIZE)));

struct dma_ring_consumer_state {
	volatile uint64_t consumer_seq;
	uint8_t reserved[56];
} __attribute__((__packed__, aligned(DMA_RING_CACHELINE_SIZE)));

_Static_assert(sizeof(struct dma_desc) == DMA_RING_CACHELINE_SIZE,
	       "dma_desc must occupy exactly one cacheline");
_Static_assert(sizeof(struct dma_ring_consumer_state) == DMA_RING_CACHELINE_SIZE,
	       "consumer state must occupy exactly one cacheline");

#endif
