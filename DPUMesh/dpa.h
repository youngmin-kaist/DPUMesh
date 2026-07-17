#ifndef DPA_H_
#define DPA_H_

#include <doca_dpa.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <doca_buf_array.h>
// #include <doca_dpa_dev.h>
// #include <doca_dpa_dev_comch_msgq.h>

#define CC_DPA_MAX_MSG_NUM  512

/* Number of DPA threads pre-created per DPU worker thread and handed out per
 * connection (each worker owns a private pool - shared-nothing design) */
#define DPA_THREAD_POOL_SIZE 8

struct objects;
struct doca_comch_connection;

/* DOCA DPA thread related objects */
struct dmesh_doca_dpa_thread {
    struct doca_dpa *dpa;           /* DOCA DPA */
    struct doca_dpa_thread *thread; /* DPA thread */
    doca_dpa_dev_uintptr_t arg;     /* argument to be used by DPA thread */
    doca_dpa_dev_uintptr_t buf;     /* buffer to be used by DPA thread */
	doca_dpa_dev_buf_arr_t dpa_buf_arr; /* DPA buffer array */
};

/* Pool of pre-created DPA threads, all sharing one doca_dpa instance. Threads
 * are created before any host connection exists; each remote connection is
 * assigned one thread (owner[i] tracks which connection holds slot i). */
struct dmesh_dpa_thread_pool {
    struct doca_dpa *dpa;                          /* shared DPA instance */
    int size;                                      /* number of usable slots */
    struct dmesh_doca_dpa_thread threads[DPA_THREAD_POOL_SIZE];
    struct doca_comch_connection *owner[DPA_THREAD_POOL_SIZE]; /* NULL = free */
};

struct dmesh_doca_dpa_msgq {
	struct doca_pe *pe;
	struct doca_comch_msgq *msgq;	      /**< The DOCA Comch MsgQ */
	struct doca_comch_producer *producer; /**< The DOCA Comch Producer */
	struct doca_comch_consumer *consumer; /**< The DOCA Comch Consumer */
	bool is_send;			      /**< Indicates if MsgQ is used for sending from DPU to DPA */
	
	/* variables to measure latency of msgq */	
	long unsigned int send_start_time_ns;  /**< Timestamp when the first send message is posted */
	long unsigned int send_end_time_ns;	/**< Timestamp when the last send message is completed */
	int msg_cnt;
	uint64_t total_ns;
	bool completed;
};

struct dmesh_doca_dpa_comch {
	struct dmesh_doca_dpa_msgq send;			      /**< MsgQ used to send message from DPU to DPA */
	struct doca_dpa_completion *producer_comp;	      /**< The producer completion context used by DPA */
	struct dmesh_doca_dpa_msgq recv;			      /**< MsgQ used to receive message DPA */
	struct doca_comch_consumer_completion *consumer_comp; /**< The consumer completion context used by DPA */
};

struct dmesh_doca_dpa_msgq_create_attr {
	struct doca_dev *dev; /**< A doca device representing the emulation manager */
	struct doca_dpa *dpa; /**< DOCA DPA for accessing DPA resources */
	bool is_send;	      /**< If MsgQ is used to send to DPA or receive from DPA */
	uint32_t max_num_msg; /**< The maximal number of messages that can be sent/received */
	struct doca_comch_consumer_completion *consumer_comp; /**< Consumer completion context used by DPA to poll
								 arrival of messages */
	struct doca_dpa_completion *producer_comp; /**< Producer completion context used by DPA to poll completion of
						      send message */
	struct doca_pe *pe;			   /**< Progress engine to be used by DPU consumer and producer */
	doca_ctx_state_changed_callback_t ctx_state_changed_cb; /**< Callback invoked once consumer/producer state
								   changes */
	void *ctx_user_data; /**< The user data to associate with the producer/consumer */
};

void dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb(const union doca_data user_data,
							  struct doca_ctx *ctx,
							  enum doca_ctx_states prev_state,
							  enum doca_ctx_states next_state);

doca_error_t
init_dpa_objects(struct objects *objs);

/* Pre-create all pool threads on the shared DPA instance (call once, after
 * init_dpa_objects and before any host connection is served). */
doca_error_t
dmesh_dpa_thread_pool_init(struct objects *objs);

/* Assign a free pool thread to a connection; returns NULL if the pool is
 * exhausted or not yet initialized. Pure memory operation - safe to call from
 * a DOCA event callback. */
struct dmesh_doca_dpa_thread *
dmesh_dpa_thread_pool_alloc(struct objects *objs, struct doca_comch_connection *conn);

/* Return the thread owned by a connection back to the pool (no-op if none). */
void
dmesh_dpa_thread_pool_release(struct objects *objs, struct doca_comch_connection *conn);

doca_error_t
launch_dpa_kernel(struct dmesh_doca_dpa_thread *dpa_thread);

doca_error_t
dmesh_doca_dpa_msgq_create(const struct dmesh_doca_dpa_msgq_create_attr *attr,
                            struct dmesh_doca_dpa_msgq *msgq);

doca_error_t
dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *dpa_thread);

struct objects;
struct dmesh_conn;
doca_error_t
dmesh_doca_dpa_comch_create(struct dmesh_conn *conn);

doca_error_t
dmesh_doca_run_dpa_thread(struct dmesh_conn *conn);

doca_error_t 
dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size);

doca_error_t 
dmesh_doca_dpa_msgq_send_bulk(struct dmesh_doca_dpa_msgq *msgq, uint32_t num_msg,
                                void *msg, uint32_t msg_size);

doca_error_t
setup_dpa_buf_array(struct dmesh_conn *conn, size_t num_elem, struct doca_mmap *mmap);					
#endif