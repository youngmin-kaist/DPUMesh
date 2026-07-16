#ifndef OBJECT_H_
#define OBJECT_H_

#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_dma.h>

#include <sys/queue.h>
#include "buffer.h"
#include "comch_server.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct dma_ring;
struct objects;
struct dma_task_entry {
    struct objects *owner;
    struct doca_dma_task_memcpy *task;
    doca_error_t result;
    bool in_free_queue;
    bool in_submission_queue;
    bool in_flight;
    TAILQ_ENTRY(dma_task_entry) entries;
};
TAILQ_HEAD(dma_task_queue, dma_task_entry);

typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

#define MAX_CONSUMERS 16

struct objects {
    struct doca_dev *dev;
    struct doca_dev_rep *rep_dev;
    struct doca_pe *pe;
    union {
        struct doca_comch_server *cc_server;
        struct doca_comch_client *cc_client;
    };
    struct doca_comch_connection *connection;

    /* DOCA DMA related objects */
    struct doca_dma *dma_ctx;
    struct dma_task_entry *dma_task_entries;
    int num_dma_tasks;
    int num_free_dma_tasks;
    int num_submission_dma_tasks;
    struct dma_task_queue free_dma_tasks;       
    struct dma_task_queue submission_dma_tasks;

    /* DMesh application recv/send buffers */
    struct dmesh_buffer sndbuf;
    struct dmesh_buffer rcvbuf;

    struct doca_buf_inventory *buf_inv;
    struct doca_mmap *local_mmap;
    struct doca_mmap *remote_mmap;
    void *dma_buffer;
    void *remote_addr;
    size_t remote_buf_size;
    struct dma_ring *dma_ring;
    struct doca_mmap *ring_mmap;    /* used for DMA ring mmap in DPU */

    struct doca_buf_arr *buf_arr;

    struct dmesh_doca_dpa_thread *dpa_thread;
	struct dmesh_doca_dpa_comch *dpa_comch;
    doca_dpa_dev_comch_producer_t remote_dpa_producer;
    doca_dpa_dev_completion_t remote_dpa_producer_comp;

    /* comch control path related */
    bool server_finish;             /* Controls whether server progress loop should be run */

    /* comch data path related */
    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    struct doca_pe *consumer_pe;
    
    struct local_mem_bufs *producer_mem;
    struct doca_comch_producer *producer;
    struct doca_pe *producer_pe;
    
    uint32_t remote_consumer_id;
    doca_error_t producer_result;		  /* Holds result will be updated in producer callbacks */
	bool producer_finish;			  /* Controls whether producer progress loop should be run */
	doca_error_t consumer_result;		  /* Holds result will be updated in consumer callbacks */
	bool consumer_finish;			  /* Controls whether consumer progress loop should be run */
    
    int recv_msg_cnt;                  /* Counts number of messages received by consumer */
    int sent_msg_cnt;

    long unsigned int start_time_ns;
    long unsigned int end_time_ns;
};

void
cleanup_objects(struct objects *objs);

#endif // OBJECT_H_
