#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

#include <stdint.h>
#include <doca_mmap.h>

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

struct dpa_thread_arg {
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;
	doca_dpa_dev_buf_arr_t dpa_buf_arr;
	uint32_t buf_arr_size;

    doca_dpa_dev_mmap_t host_mmap;

	doca_dpa_dev_mmap_t dpu_mmap;
	uint64_t src_addr;
	uint32_t buf_size;
	uint32_t pos;

	/* producer_dma_copy microbenchmark (DMESH_DPA_BENCH_* env vars on the DPU
	 * app; bench_mode 0 = off -> normal ring-polling datapath) */
	uint64_t bench_host_addr;   /* host sndbuf base VA (DMA source) */
	uint32_t bench_host_size;   /* host sndbuf length */
	uint32_t bench_mode;        /* 0=off, 1=throughput, 2=latency */
	uint32_t bench_msg_size;    /* bytes per copy (max 8192 on this platform) */
	uint32_t bench_num_ops;     /* copies per run */

	/* Cooperative shutdown: the host sets stop=1 (h2d_memcpy) when tearing the
	 * connection down; the kernel's poll loop observes it, writes stopped=1
	 * back (window writeback) and returns WITHOUT rescheduling, so the DPA
	 * thread goes idle and doca_dpa_thread_stop can quiesce it cleanly. A
	 * hot-looping (never-rescheduling) thread cannot be stopped by flexio. */
	volatile uint32_t stop;     /* host -> DPA: leave the poll loop */
	volatile uint32_t stopped;  /* DPA -> host: poll loop has exited */

} __attribute__((__packed__, aligned(8)));

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
};

struct comch_dma_comp_msg {
	enum comch_msg_type type;
	uint32_t pos;       /* staging offset of the (batched) copy */
	uint32_t length;    /* total bytes covered by this message */
	uint32_t count;     /* number of descriptors coalesced into this copy */
};

typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;

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

struct dma_ring_ctrl {
	volatile uint64_t producer_tail;
	volatile uint64_t consumer_head;
	uint8_t reserved[48];
} __attribute__((aligned(64)));

struct dma_desc {
	doca_dpa_dev_mmap_t mmap; 	// 4B
	uint64_t addr;			   // 8B
	size_t size;				   // 8B
	uint64_t idx;		   // 8B
	uint8_t reserved[35];	   // 35B
	volatile uint8_t valid;		   // 1B
} __attribute__((__packed__, aligned(8)));

#endif
