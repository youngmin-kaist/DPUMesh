#include "dpa.h"

#include <doca_error.h>
#include <doca_log.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_comch_msgq.h>
#include <doca_buf_array.h>
#include <doca_mmap.h>

#include "object.h"
#include "dpa_common.h"
#include "ring.h"
#include "grpc/grpc_offload.h"
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

DOCA_LOG_REGISTER(DPA);

/* Kernel function declaration */
extern doca_dpa_func_t hello_world;
extern doca_dpa_func_t run_dma_manager;
extern doca_dpa_func_t run_grpc_desc_main;
extern doca_dpa_func_t run_grpc_msg_worker;
extern doca_dpa_func_t run_grpc_serializer_worker;
extern doca_dpa_func_t thread_init_rpc;

extern struct doca_dpa_app *DPU_mesh_dpa_app;

#define TEST_DPA_MEMORY

static doca_error_t
dmesh_doca_dpa_notification_create(struct dmesh_doca_dpa_thread *dpa_thread,
				   uint32_t thread_idx)
{
	doca_error_t result;

	result = doca_dpa_notification_completion_create(dpa_thread->dpa,
							 dpa_thread->threads[thread_idx],
							 &dpa_thread->notify_comps[thread_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create DPA notification completion for thread %u: %s",
			     thread_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_notification_completion_start(dpa_thread->notify_comps[thread_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start DPA notification completion for thread %u: %s",
			     thread_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_notification_completion_get_dpa_handle(
		dpa_thread->notify_comps[thread_idx],
		&dpa_thread->notify_handles[thread_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA notification handle for thread %u: %s",
			     thread_idx, doca_error_get_descr(result));
		return result;
	}

	return DOCA_SUCCESS;
}

static doca_error_t
dmesh_doca_dpa_copy_async_create(struct dmesh_doca_dpa_thread *dpa_thread,
				 uint32_t serializer_idx,
				 uint32_t thread_idx)
{
	doca_error_t result;

	result = doca_dpa_completion_create(dpa_thread->dpa,
					    DMESH_GRPC_SERIALIZER_QUEUE_DEPTH,
					    &dpa_thread->copy_comps[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create DPA copy completion for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_completion_set_thread(dpa_thread->copy_comps[serializer_idx],
						dpa_thread->threads[thread_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set DPA copy completion thread for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_completion_start(dpa_thread->copy_comps[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start DPA copy completion for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	/*
	 * Each serializer owns one async-ops queue and waits on its own
	 * completion before publishing TASK_STATE_COMPLETED.
	 */
	result = doca_dpa_async_ops_create(dpa_thread->dpa,
					   DMESH_GRPC_SERIALIZER_QUEUE_DEPTH,
					   serializer_idx,
					   &dpa_thread->copy_async_ops[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create DPA copy async ops for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_async_ops_attach(dpa_thread->copy_async_ops[serializer_idx],
					   dpa_thread->copy_comps[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to attach DPA copy async ops for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_async_ops_start(dpa_thread->copy_async_ops[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start DPA copy async ops for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_completion_get_dpa_handle(dpa_thread->copy_comps[serializer_idx],
						   &dpa_thread->copy_comp_handles[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA copy completion handle for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_async_ops_get_dpa_handle(dpa_thread->copy_async_ops[serializer_idx],
						  &dpa_thread->copy_async_ops_handles[serializer_idx]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA copy async ops handle for serializer %u: %s",
			     serializer_idx, doca_error_get_descr(result));
		return result;
	}

	return DOCA_SUCCESS;
}

struct dmesh_doca_dpa_msgq_send_ctx {
    struct dmesh_doca_dpa_msgq *msgq;
    uint32_t msg_size;
    uint8_t msg_data[];
};

/*
 * Callback invoked once a message is received from DPA successfully
 *
 * @recv_task [in]: The receive task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the consumer context
 */
static void dmesh_doca_dpa_msgq_recv_cb(struct doca_comch_consumer_task_post_recv *recv_task,
				       union doca_data task_user_data,
				       union doca_data ctx_user_data)
{
	(void)task_user_data;
    
    doca_error_t result;
    uint32_t data_len;
    struct comch_msg *msg;

	struct objects *objs = ctx_user_data.ptr;
	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);
    
    data_len = doca_comch_consumer_task_post_recv_get_imm_data_len(recv_task);
    msg = (struct comch_msg *)doca_comch_consumer_task_post_recv_get_imm_data(recv_task);

    switch (msg->type) {
        case COMCH_MSG_TYPE_DMA_COMPLETED: {
// #if DEBUG_LOG
//             struct comch_dma_comp_msg *comp_msg = (struct comch_dma_comp_msg *)msg;
//             if (comp_msg->idx == 0) {
//                 DOCA_LOG_INFO("Received DMA completed message: desc_idx=%lu pos=%u length=%u",
//                             comp_msg->idx, comp_msg->pos, comp_msg->length);
//             }
// #endif
            break;
        }
        case COMCH_MSG_TYPE_GRPC_DMA_COMPLETED: {
            struct comch_grpc_dma_comp_msg *comp_msg = (struct comch_grpc_dma_comp_msg *)msg;

            result = dmesh_grpc_handle_dma_completion(objs, comp_msg);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to handle gRPC DMA completion: %s",
                            doca_error_get_name(result));
            }

            break;
        }
        case COMCH_MSG_TYPE_GRPC_SERIALIZE_COMPLETED: {
            struct comch_grpc_serialize_comp_msg *comp_msg = (struct comch_grpc_serialize_comp_msg *)msg;
#if DEBUG_LOG            
            if (comp_msg->ring_seq <= 8U || (comp_msg->ring_seq % DEBUG_INTERVAL) == 0U) {
                DOCA_LOG_INFO("gRPC serialize complete:\nreq=%u completed=%u encoded_len=%u data_len=%u",
                            comp_msg->request_id, comp_msg->completed, comp_msg->encoded_len, data_len);
            }
#endif
            // recall that we piggybacked the number of completed descriptors in the status field
            objs->recv_msg_cnt += comp_msg->completed;
            break;
        }
        default:
            DOCA_LOG_ERR("Received unknown message type: %u", msg->type);
            break;
    }

	result = doca_task_submit(task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("DPA MsgQ receive callback failed: Failed to resubmit receive task - %s",
			     doca_error_get_name(result));
		doca_task_free(task);
	}
}

/*
 * Callback invoked once consumer encounters a receive error
 *
 * @recv_task [in]: The receive task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the consumer context
 */
static void dmesh_doca_dpa_msgq_recv_error_cb(struct doca_comch_consumer_task_post_recv *recv_task,
					     union doca_data task_user_data,
					     union doca_data ctx_user_data)
{
	(void)task_user_data;
	(void)ctx_user_data;

	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);

	doca_task_free(task);
}
/*
 * Callback invoked once a message is sent to DPA successfully
 *
 * @send_task [in]: The send task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the producer context
 */
static void dmesh_doca_dpa_msgq_send_cb(struct doca_comch_producer_task_send *send_task,
				       union doca_data task_user_data,
				       union doca_data ctx_user_data)
{
    // struct dmesh_doca_dpa_msgq_send_ctx *send_ctx = task_user_data.ptr;
    
    struct objects *objs = (struct objects *)ctx_user_data.ptr;
    if (objs != NULL)
        objs->sent_msg_cnt++;
    
    // DOCA_LOG_INFO("Sent msg to DPA successfully, cnt: %d", objs->sent_msg_cnt);
    
	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    doca_task_free(task);

    /* Below code is used to resubmit the send task if needed, 
     * but this leads to deadlock in our current design
    do {
        result = doca_task_submit(task);
    } while (result == DOCA_ERROR_AGAIN);
    
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to resubmit msgQ send task - %s",
            doca_error_get_name(result));
            doca_task_free(task);
    }
    */
}

/*
 * Callback invoked once producer encounters a send error
 *
 * @send_task [in]: The send task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the producer context
 */
static void dmesh_doca_dpa_msgq_send_error_cb(struct doca_comch_producer_task_send *send_task,
					     union doca_data task_user_data,
					     union doca_data ctx_user_data)
{
    // struct dmesh_doca_dpa_msgq_send_ctx *send_ctx = task_user_data.ptr;
	(void)ctx_user_data;

	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    DOCA_LOG_ERR("Failed to send msg");
	doca_task_free(task);
}

/*
 * Callback invoked once consumer/producer state changes
 *
 * @user_data [in]: The user data associated with the context
 * @ctx [in]: The consumer/producer context
 * @prev_state [in]: The previous state
 * @next_state [in]: The new state
 */
void dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb(const union doca_data user_data,
							  struct doca_ctx *ctx,
							  enum doca_ctx_states prev_state,
							  enum doca_ctx_states next_state)
{
	(void)ctx;
	(void)prev_state;

	// struct nvmf_doca_io *io = user_data.ptr;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
        DOCA_LOG_ERR("DPA comch msgQ state is idle.");
		// nvmf_doca_dpa_comch_stop_continue(&io->comch);
		break;
    case DOCA_CTX_STATE_STARTING:
    case DOCA_CTX_STATE_RUNNING:
        DOCA_LOG_ERR("DPA comch msgQ state is running.");
	case DOCA_CTX_STATE_STOPPING:
	default:
		break;
	}
}

doca_error_t
init_dpa_objects(struct objects *objs)
{
    doca_error_t result;
    // struct dmesh_doca_dpa_thread *dpa_thread;

    if (!objs->dpa_thread) {
		objs->dpa_thread = malloc(sizeof(struct dmesh_doca_dpa_thread));
		if (!objs->dpa_thread) {
			DOCA_LOG_ERR("Failed to allocate memory for dpa_thread");
			return DOCA_ERROR_NO_MEMORY;
		}
	}

	if (!objs->dpa_comch) {
		objs->dpa_comch = malloc(sizeof(struct dmesh_doca_dpa_comch));
		if (!objs->dpa_comch) {
			DOCA_LOG_ERR("Failed to allocate memory for dpa_comch");
			return DOCA_ERROR_NO_MEMORY;
		}
	}

    result = doca_dpa_create(objs->dev, &objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DOCA DPA with error = %s", doca_error_get_name(result));
        return result;
    }

    result = doca_dpa_set_app(objs->dpa_thread->dpa, DPU_mesh_dpa_app);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA application with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    result = doca_dpa_start(objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DOCA DPA with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    DOCA_LOG_INFO("Init DOCA DPA done.");
    return DOCA_SUCCESS;

destroy_dpa:
    doca_dpa_destroy(objs->dpa_thread->dpa);
    objs->dpa_thread->dpa = NULL;
    return result;
}

doca_error_t
launch_dpa_kernel(struct dmesh_doca_dpa_thread *dpa_thread)
{
    doca_error_t result;

    result = doca_dpa_kernel_launch_update_set(dpa_thread->dpa, 
                    NULL, 0,
                    NULL, 0,
                    1,
                    &hello_world);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to launch DPA kernel with error = %s", doca_error_get_name(result));
        return result;
    }

    return DOCA_SUCCESS;
}

static doca_error_t
dmesh_doca_dpa_thread_set_eu_affinity(struct dmesh_doca_dpa_thread *dpa_thread,
                                      uint32_t tid,
                                      unsigned int eu_id)
{
    doca_error_t result;

    result = doca_dpa_eu_affinity_create(dpa_thread->dpa,
                                         &dpa_thread->affinities[tid]);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DPA EU affinity for thread %u: %s",
                     tid, doca_error_get_descr(result));
        return result;
    }

    result = doca_dpa_eu_affinity_set(dpa_thread->affinities[tid], eu_id);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA EU affinity for thread %u to EU %u: %s",
                     tid, eu_id, doca_error_get_descr(result));
        return result;
    }

    result = doca_dpa_thread_set_affinity(dpa_thread->threads[tid],
                                          dpa_thread->affinities[tid]);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to attach DPA EU affinity to thread %u, EU %u: %s",
                     tid, eu_id, doca_error_get_descr(result));
        return result;
    }

    DOCA_LOG_INFO("Pinned DPA thread %u to DPA EU %u", tid, eu_id);
    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *dpa_thread)
{
	doca_error_t result;
	struct dpa_grpc_pipeline_state *zero_state;
	uint32_t i;

    uint32_t num_cores = 0;
    uint32_t eus_per_core = 0;
    uint32_t total_eus = 0;

    result = doca_dpa_get_core_num(dpa_thread->dpa, &num_cores);
    if (result != DOCA_SUCCESS)
        return result;

    result = doca_dpa_get_num_eus_per_core(dpa_thread->dpa, &eus_per_core);
    if (result != DOCA_SUCCESS)
        return result;

    result = doca_dpa_get_total_num_eus_available(dpa_thread->dpa, &total_eus);
    if (result != DOCA_SUCCESS)
        return result;

    DOCA_LOG_INFO("DPA resources: cores=%u, eus_per_core=%u, total_eus=%u",
                num_cores, eus_per_core, total_eus);
    DOCA_LOG_INFO("DPA gRPC config: serializers=%u, pipeline_profile=%u, profile_interval=%u",
                DMESH_GRPC_SERIALIZER_THREADS,
                DMESH_GRPC_PIPELINE_PROFILE,
                DMESH_GRPC_PROFILE_LOG_INTERVAL);

    if (total_eus < DMESH_DPA_THREAD_COUNT) {
        DOCA_LOG_WARN("DPA EUs fewer than DPA threads: total_eus=%u, threads=%u",
                    total_eus, DMESH_DPA_THREAD_COUNT);
    }

	result = doca_dpa_mem_alloc(dpa_thread->dpa,
				    sizeof(struct dpa_thread_arg) * DMESH_DPA_THREAD_COUNT,
				    &dpa_thread->arg);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to alloc dpa mem: %s",
		    doca_error_get_descr(result));
		return result;
	}

	result = doca_dpa_mem_alloc(dpa_thread->dpa,
				    sizeof(struct dpa_grpc_pipeline_state),
				    &dpa_thread->shared_state);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to alloc DPA gRPC pipeline state: %s",
			     doca_error_get_descr(result));
		return result;
	}

	zero_state = calloc(1, sizeof(*zero_state));
	if (zero_state == NULL)
		return DOCA_ERROR_NO_MEMORY;

    for (i = 0; i < DMESH_GRPC_SERIALIZER_THREADS; ++i) {
        zero_state->serializer_drr_deficit[i] = DMESH_GRPC_SERIALIZER_DRR_QUANTUM;
    }

	result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->shared_state,
				     zero_state, sizeof(*zero_state));
	free(zero_state);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to zero DPA gRPC pipeline state: %s",
			     doca_error_get_descr(result));
		return result;
	}

    /* set EU affinity */
    for (i = 0; i < DMESH_DPA_THREAD_COUNT; ++i) {
		doca_dpa_func_t *func;
		doca_dpa_dev_uintptr_t arg_addr =
			dpa_thread->arg + ((doca_dpa_dev_uintptr_t)i * sizeof(struct dpa_thread_arg));

        switch (i) {
            case DMESH_DPA_THREAD_MAIN:
                func = run_grpc_desc_main;
                break;
            case DMESH_DPA_THREAD_MSG:
                func = run_grpc_msg_worker;
                break;
            default:
                func = run_grpc_serializer_worker;
        }

		result = doca_dpa_thread_create(dpa_thread->dpa, &dpa_thread->threads[i]);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to create DPA thread %u: %s",
				     i, doca_error_get_descr(result));
			return result;
		}

		result = doca_dpa_thread_set_func_arg(dpa_thread->threads[i], func, arg_addr);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to set DPA thread %u func: %s",
				     i, doca_error_get_descr(result));
			return result;
		}


			result = doca_dpa_thread_start(dpa_thread->threads[i]);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to start DPA thread %u: %s",
				     i, doca_error_get_descr(result));
			return result;
		}

		if (i >= DMESH_DPA_THREAD_SERIALIZER_BASE) {
			uint32_t serializer_idx = i - DMESH_DPA_THREAD_SERIALIZER_BASE;

			result = dmesh_doca_dpa_copy_async_create(dpa_thread,
								  serializer_idx,
								  i);
			if (result != DOCA_SUCCESS)
				return result;
		}

		/*
		 * The msg thread is also the gRPC dispatcher. It owns the Comch
		 * completions, and additionally needs a notification completion so the
		 * main descriptor thread can wake it for shared-memory dispatch work.
		 * The dispatcher must stay resident after wakeup; rescheduling it would
		 * make queue progress depend on a later Comch event.
		 */
		result = dmesh_doca_dpa_notification_create(dpa_thread, i);
		if (result != DOCA_SUCCESS)
			return result;
	}

	dpa_thread->thread = dpa_thread->threads[DMESH_DPA_THREAD_MAIN];

	return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_msgq_create(const struct dmesh_doca_dpa_msgq_create_attr *attr,
                            struct dmesh_doca_dpa_msgq *msgq)
{
    doca_error_t result;
    struct doca_ctx *consumer_ctx;
    struct doca_ctx *producer_ctx;
    uint32_t max_num_producers;

    memset(msgq, 0, sizeof(*msgq));

    msgq->is_send = attr->is_send;

    if (msgq->pe == NULL) {
        result = doca_pe_create(&msgq->pe);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create PE - %s",
                    doca_error_get_name(result));
            return result;
        }
    }

    result = doca_comch_msgq_create(attr->dev, &msgq->msgq);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create comch msgq - %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_msgq_set_max_num_consumers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num consumers - %s",
                doca_error_get_name(result));
        return result;
    }

    max_num_producers = 1U;
    if (attr->is_send == false)
        max_num_producers += attr->num_serializer_producers;

    result = doca_comch_msgq_set_max_num_producers(msgq->msgq, max_num_producers);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num producers - %s",
                doca_error_get_name(result));
        return result;
    }
    
    /* if true, DPA is consumer */
    if (attr->is_send) {
        result = doca_comch_msgq_set_dpa_consumer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set dpa consumer - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* else, DPA is producer */
        result = doca_comch_msgq_set_dpa_producer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set dpa producer - %s",
                    doca_error_get_name(result));
            return result;
        }
    }
    
    result = doca_comch_msgq_start(msgq->msgq);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start msgq - %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_msgq_consumer_create(msgq->msgq, &msgq->consumer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create msgq consumer - %s",
                doca_error_get_name(result));
        return result;
    }
    
    consumer_ctx = doca_comch_consumer_as_ctx(msgq->consumer);
    result = doca_comch_consumer_set_imm_data_len(msgq->consumer, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len to 64 - %s",
                doca_error_get_name(result));
        return result;
    }
    
    if (attr->is_send) {
        /* consumer on DPA */
        result = doca_ctx_set_datapath_on_dpa(consumer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer datapath on dpa - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_set_completion(msgq->consumer, attr->consumer_comp, 0);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer completion - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_set_dev_max_num_recv(msgq->consumer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer max # of recv messages - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* consumer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(consumer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer ctx user data - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_ctx_set_state_changed_cb(consumer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set state changed cb - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, consumer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to connect consumer to pe - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_task_post_recv_set_conf(msgq->consumer,
                                        dmesh_doca_dpa_msgq_recv_cb,
                                        dmesh_doca_dpa_msgq_recv_error_cb,
                                        attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer task config - %s",
                    doca_error_get_name(result));
            return result;
        }
    }

    result = doca_ctx_start(consumer_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start consumer ctx - %s", 
                doca_error_get_name(result));
        return result;
    }

    if (attr->is_send) {
        /* producer on DPU */
        union doca_data ctx_user_data;

        result = doca_comch_msgq_producer_create(msgq->msgq, &msgq->producer);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create msgq producer - %s",
                    doca_error_get_name(result));
            return result;
        }

        producer_ctx = doca_comch_producer_as_ctx(msgq->producer);
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(producer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer ctx user data - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_ctx_set_state_changed_cb(producer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set state changed cb - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, producer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to connect producer to pe - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_task_send_set_conf(msgq->producer,
                                dmesh_doca_dpa_msgq_send_cb,
                                dmesh_doca_dpa_msgq_send_error_cb,
                                attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer task config - %s", 
                    doca_error_get_name(result));
            return result;
        }

        result = doca_ctx_start(producer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to start producer ctx - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* producer on DPA */
        result = doca_comch_msgq_producer_create(msgq->msgq, &msgq->producer);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create msgq producer - %s",
                    doca_error_get_name(result));
            return result;
        }

        producer_ctx = doca_comch_producer_as_ctx(msgq->producer);
        result = doca_ctx_set_datapath_on_dpa(producer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer datapath on dpa - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_set_dev_max_num_send(msgq->producer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer max # of send messages - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_dpa_completion_attach(msgq->producer, attr->producer_comp);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to attach producer dpa completion - %s", 
                    doca_error_get_name(result));
            return result;
        }

        result = doca_ctx_start(producer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to start producer ctx - %s",
                    doca_error_get_name(result));
            return result;
        }

        for (uint32_t idx = 0; idx < attr->num_serializer_producers; ++idx) {
            struct doca_comch_producer **producer = &msgq->serializer_producers[idx];

            result = doca_comch_msgq_producer_create(msgq->msgq, producer);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to create serializer msgq producer %u - %s",
                        idx, doca_error_get_name(result));
                return result;
            }

            producer_ctx = doca_comch_producer_as_ctx(*producer);
            result = doca_ctx_set_datapath_on_dpa(producer_ctx, attr->dpa);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to set serializer producer %u datapath on dpa - %s",
                        idx, doca_error_get_name(result));
                return result;
            }
            result = doca_comch_producer_set_dev_max_num_send(*producer, attr->max_num_msg);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to set serializer producer %u max # of send messages - %s",
                        idx, doca_error_get_name(result));
                return result;
            }
            result = doca_comch_producer_dpa_completion_attach(*producer,
                    attr->serializer_producer_comps[idx]);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to attach serializer producer %u dpa completion - %s",
                        idx, doca_error_get_name(result));
                return result;
            }
            result = doca_ctx_start(producer_ctx);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to start serializer producer %u ctx - %s",
                        idx, doca_error_get_name(result));
                return result;
            }
        }
    }

    /* Pre-post recv tasks if MsgQ is used for receiving from DPA */
    if (attr->is_send == false) {
        for (uint32_t idx = 0; idx < attr->max_num_msg; idx++) {
            struct doca_comch_consumer_task_post_recv *recv_task;
            result = doca_comch_consumer_task_post_recv_alloc_init(msgq->consumer, NULL, &recv_task);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to allocate consumer post recv task - %s",
                        doca_error_get_name(result));
                return result;
            }
            result = doca_task_submit(doca_comch_consumer_task_post_recv_as_task(recv_task));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to submit consumer post recv task - %s",
                        doca_error_get_name(result));
                return result;
            }
        }
    }

    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_comch_create(struct objects *objs)
{
    struct dmesh_doca_dpa_comch *comch = objs->dpa_comch;
    struct dmesh_doca_dpa_thread *dpa_thread = objs->dpa_thread;
    // uint32_t max_num_recv, imm_data_len;
    doca_error_t result;
    
    memset(comch, 0, sizeof(*comch));

    result = doca_comch_consumer_completion_create(&(comch->consumer_comp));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create consumer completion - %s",
                doca_error_get_name(result));
        assert(0);
        return result;
    }
    
    result = doca_comch_consumer_completion_set_max_num_recv(comch->consumer_comp,
            CC_DPA_MAX_MSG_NUM);    
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num recv - %s",
            doca_error_get_name(result));
        return result;
    }

    result = doca_comch_consumer_completion_set_imm_data_len(comch->consumer_comp, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len - %s",
            doca_error_get_name(result));
        return result;
        }
        
    result = doca_comch_consumer_completion_set_dpa_thread(
	    comch->consumer_comp,
	    dpa_thread->threads[DMESH_DPA_THREAD_MSG]);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set dpa thread - %s",
            doca_error_get_name(result));
        return result;
    }

    result = doca_comch_consumer_completion_start(comch->consumer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start consumer completion - %s",
			     doca_error_get_name(result));
		return result;
	}

    result = doca_dpa_completion_create(dpa_thread->dpa, CC_DPA_MAX_MSG_NUM, &comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create producer completion - %s",
                doca_error_get_name(result));
        return result;
    }
    /*
     * Keep the primary producer completion attached to the msg/dispatcher
     * thread. Serializer DMA producers use separate completions below so each
     * serializer waits only on completions generated by its own producer.
     */
    result = doca_dpa_completion_set_thread(comch->producer_comp,
					    dpa_thread->threads[DMESH_DPA_THREAD_MSG]);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set dpa thread to producer completion - %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_start(comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start producer completion - %s",
                doca_error_get_name(result));
        return result;
    }

    for (uint32_t idx = 0; idx < DMESH_GRPC_SERIALIZER_THREADS; ++idx) {
        uint32_t thread_idx = DMESH_DPA_THREAD_SERIALIZER_BASE + idx;

        result = doca_dpa_completion_create(dpa_thread->dpa,
                CC_DPA_MAX_MSG_NUM,
                &comch->serializer_producer_comps[idx]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create serializer producer completion %u - %s",
                    idx, doca_error_get_name(result));
            return result;
        }

        result = doca_dpa_completion_set_thread(comch->serializer_producer_comps[idx],
                dpa_thread->threads[thread_idx]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set serializer producer completion %u thread - %s",
                    idx, doca_error_get_name(result));
            return result;
        }

        result = doca_dpa_completion_start(comch->serializer_producer_comps[idx]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to start serializer producer completion %u - %s",
                    idx, doca_error_get_name(result));
            return result;
        }
    }

    return DOCA_SUCCESS;
}

/*
 * Fills the DPA thread argument with the relevant DPA handles to be later copied to the DPA thread
 *
 * @arg [out]: The returned thread argument that was filled
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t 
dmesh_fill_dpa_thread_arg(struct objects *objs, struct dpa_thread_arg *arg)
{
    doca_error_t result;
    struct dmesh_doca_dpa_comch *comch;
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_comch_consumer_t dpa_consumer;
#ifdef DOCA_ARCH_DPU
    doca_dpa_dev_buf_arr_t dpa_buf_arr;
    doca_dpa_dev_buf_arr_t dpa_consumer_state_buf_arr;
    doca_dpa_dev_buf_arr_t dpa_host_mmap_buf_arr;
    doca_dpa_dev_buf_arr_t dpa_dpu_mmap_buf_arr;
    doca_dpa_dev_mmap_t dpu_mmap, host_mmap;
#endif

    comch = objs->dpa_comch;

    result = doca_comch_consumer_completion_get_dpa_handle(comch->consumer_comp, &dpa_consumer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer completion DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_get_dpa_handle(comch->producer_comp, &dpa_producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer completion DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_consumer_get_dpa_handle(comch->send.consumer, &dpa_consumer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_comch_producer_get_dpa_handle(comch->recv.producer, &dpa_producer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer DPA handle: %s",   
                doca_error_get_name(result));
        return result;
    }
    
#ifdef DOCA_ARCH_DPU
    result = doca_buf_arr_get_dpa_handle(objs->buf_arr, &dpa_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get buf array DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_buf_arr_get_dpa_handle(objs->consumer_state_buf_arr, &dpa_consumer_state_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer state buf array DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_buf_arr_get_dpa_handle(objs->host_mmap_buf_arr, &dpa_host_mmap_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get host mmap buf array DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_buf_arr_get_dpa_handle(objs->dpu_mmap_buf_arr, &dpa_dpu_mmap_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get DPU mmap buf array DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &dpu_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get mmap DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(objs->remote_mmap, objs->dev, &host_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get mmap DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

#endif

	    *arg = (struct dpa_thread_arg) {
	        .dpa_consumer_comp = dpa_consumer_comp,
	        .dpa_producer_comp = dpa_producer_comp,
	        .dpa_consumer = dpa_consumer,
	        .dpa_producer = dpa_producer,
#ifdef DOCA_ARCH_DPU
            .dpa_buf_arr = dpa_buf_arr,
            .dpa_consumer_state_buf_arr = dpa_consumer_state_buf_arr,
            .buf_arr_size = DMA_RING_SIZE,
            .host_mmap = host_mmap,
            .dpu_mmap = dpu_mmap,
            .src_addr = (uint64_t)objs->dma_buffer,
            .buf_size = BUFFER_SIZE,
            .pos = 0,
            .dpa_host_mmap_buf_arr = dpa_host_mmap_buf_arr,
            .dpa_dpu_mmap_buf_arr = dpa_dpu_mmap_buf_arr,
            .host_base_addr = (uint64_t)objs->remote_addr,
            .dpu_base_addr = (uint64_t)objs->dma_buffer,
            .host_buf_size = (uint32_t)objs->remote_buf_size,
#endif
			.pipeline_state = objs->dpa_thread->shared_state,
			.main_notify = objs->dpa_thread->notify_handles[DMESH_DPA_THREAD_MAIN],
			.dispatcher_notify = objs->dpa_thread->notify_handles[DMESH_DPA_THREAD_MSG],
		    };

	for (uint32_t i = 0; i < DMESH_GRPC_SERIALIZER_THREADS; ++i) {
		arg->serializer_notify[i] =
			objs->dpa_thread->notify_handles[DMESH_DPA_THREAD_SERIALIZER_BASE + i];
	}

    DOCA_LOG_INFO("dpa_consumer_comp: 0x%lx, dpa_producer_comp: 0x%lx, dpa_consumer: 0x%lx, dpa_producer: 0x%lx",
        arg->dpa_consumer_comp,
        arg->dpa_producer_comp,
        arg->dpa_consumer,
        arg->dpa_producer);

    return DOCA_SUCCESS;
}

/*  
 *  Initialize and run the DOCA DPA thread
 *
 */
doca_error_t
dmesh_doca_run_dpa_thread(struct objects *objs, struct dmesh_doca_dpa_thread *dpa_thread, struct dmesh_doca_dpa_comch *comch)
{
	doca_error_t result;
	struct dpa_thread_arg arg;
	struct dpa_thread_arg thread_args[DMESH_DPA_THREAD_COUNT];
	uint32_t i;

    result = dmesh_fill_dpa_thread_arg(objs, &arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to fill dpa thread argument - %s",
            doca_error_get_name(result));
        return result;
    }

	uint64_t rpc_ret;
	uint32_t num_msg = CC_DPA_MAX_MSG_NUM;

	for (i = 0; i < DMESH_DPA_THREAD_COUNT; ++i) {
		thread_args[i] = arg;
		thread_args[i].thread_index = i;
		if (i >= DMESH_DPA_THREAD_SERIALIZER_BASE) {
			uint32_t serializer_idx = i - DMESH_DPA_THREAD_SERIALIZER_BASE;
			doca_dpa_dev_completion_t serializer_producer_comp;
			doca_dpa_dev_comch_producer_t serializer_producer;

			if (comch->serializer_producer_comps[serializer_idx] == NULL ||
			    comch->recv.serializer_producers[serializer_idx] == NULL) {
				DOCA_LOG_ERR("Missing serializer producer resources for serializer %u",
					     serializer_idx);
				return DOCA_ERROR_INVALID_VALUE;
			}

			result = doca_dpa_completion_get_dpa_handle(
				comch->serializer_producer_comps[serializer_idx],
				&serializer_producer_comp);
			if (result != DOCA_SUCCESS) {
				DOCA_LOG_ERR("Failed to get serializer producer completion %u handle: %s",
					     serializer_idx, doca_error_get_name(result));
				return result;
			}

			result = doca_comch_producer_get_dpa_handle(
				comch->recv.serializer_producers[serializer_idx],
				&serializer_producer);
			if (result != DOCA_SUCCESS) {
				DOCA_LOG_ERR("Failed to get serializer producer %u handle: %s",
					     serializer_idx, doca_error_get_name(result));
				return result;
			}

			thread_args[i].serializer_index = serializer_idx;
			thread_args[i].dpa_producer_comp = serializer_producer_comp;
			thread_args[i].dpa_producer = serializer_producer;
			thread_args[i].dpa_copy_comp = dpa_thread->copy_comp_handles[serializer_idx];
			thread_args[i].dpa_copy_async_ops =
				dpa_thread->copy_async_ops_handles[serializer_idx];
		} else {
			thread_args[i].serializer_index = UINT32_MAX;
			thread_args[i].dpa_copy_comp = 0;
			thread_args[i].dpa_copy_async_ops = 0;
		}
	}

	result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg,
				     thread_args, sizeof(thread_args));
	if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to update DPA thread argument - %s",
				     doca_error_get_name(result));
			return result;
		}
	DOCA_LOG_INFO("Copied DPA thread arguments successfully");

	for (i = 0; i < DMESH_DPA_THREAD_COUNT; ++i) {
		result = doca_dpa_thread_run(dpa_thread->threads[i]);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to run DPA thread %u - %s",
				     i, doca_error_get_name(result));
			return result;
		}
	}
	DOCA_LOG_INFO("doca_dpa_thread_run returned successfully");

	result = doca_dpa_rpc(dpa_thread->dpa,
			      thread_init_rpc,
			      &rpc_ret,
			      arg.dpa_consumer,
			      num_msg,
			      arg.main_notify);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to issue init thread RPC - %s",
			     doca_error_get_name(result));
		return result;
	}

	if (rpc_ret != 0) {
		DOCA_LOG_ERR("Failed to init thread RPC");
		return result;
	}
	DOCA_LOG_INFO("thread_init_rpc completed successfully");

    return DOCA_SUCCESS;
}


doca_error_t
dmesh_doca_dpa_msgq_pending_push(struct dmesh_doca_dpa_msgq *msgq,
				 struct comch_grpc_dma_comp_msg *comp)
{
    struct comch_grpc_serialize_req_msg *req_msg;

	if (msgq == NULL || comp == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	if (msgq->pending_count >= DMESH_DPA_MSGQ_PENDING_SEND_DEPTH) {
		DOCA_LOG_ERR("DPA MsgQ pending send queue full: depth=%u",
			     DMESH_DPA_MSGQ_PENDING_SEND_DEPTH);
		return DOCA_ERROR_NO_MEMORY;
	}

	req_msg = (struct comch_grpc_serialize_req_msg *)&msgq->pending_sends[msgq->pending_tail];
    memcpy(req_msg, comp, sizeof(struct comch_grpc_serialize_req_msg));
    req_msg->type = COMCH_MSG_TYPE_GRPC_SERIALIZE_REQ;

	msgq->pending_tail =
		(msgq->pending_tail + 1U) % DMESH_DPA_MSGQ_PENDING_SEND_DEPTH;
	msgq->pending_count++;

#if DEBUG_LOG
	if (msgq->pending_count > msgq->pending_high_watermark)
		msgq->pending_high_watermark = msgq->pending_count;
#endif

	return DOCA_SUCCESS;
}

static void
dmesh_doca_dpa_msgq_pending_pop(struct dmesh_doca_dpa_msgq *msgq)
{
	if (msgq == NULL || msgq->pending_count == 0)
		return;

	msgq->pending_head =
		(msgq->pending_head + 1U) % DMESH_DPA_MSGQ_PENDING_SEND_DEPTH;
	msgq->pending_count--;
}

static doca_error_t
dmesh_doca_dpa_msgq_submit_once(struct dmesh_doca_dpa_msgq *msgq,
				void *msg,
				uint32_t msg_size)
{
	struct doca_comch_producer_task_send *send_task;
	struct doca_task *task;
	union doca_data user_data;
	doca_error_t result;

	if (msgq == NULL || msgq->producer == NULL || msg == NULL || msg_size == 0)
		return DOCA_ERROR_INVALID_VALUE;

	result = doca_comch_producer_task_send_alloc_init(msgq->producer,
							  NULL,
							  msg,
							  msg_size,
							  /*consumer_id=*/1,
							  &send_task);
	if (result != DOCA_SUCCESS)
		return result;

	task = doca_comch_producer_task_send_as_task(send_task);
	user_data.ptr = msgq;
	doca_task_set_user_data(task, user_data);

	result = doca_task_submit(task);
	if (result != DOCA_SUCCESS)
		doca_task_free(task);

	return result;
}

// /*
//  * Send message to DPA using NVMf DOCA DPA MsgQ.
//  *
//  * This function can run from a PE callback. It must not spin on
//  * DOCA_ERROR_AGAIN because the same PE may need to run send-completion
//  * callbacks to release the credits that make the submit possible.
//  * Keep submission ownership in the DPU worker main loop so callbacks only
//  * copy the message into the pending queue.
//  */
// doca_error_t
// dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size)
// {
// 	if (msgq == NULL || msg == NULL)
// 		return DOCA_ERROR_INVALID_VALUE;

// 	return dmesh_doca_dpa_msgq_pending_push(msgq, msg, msg_size);
// }

#define DMESH_DPA_MSGQ_DRAIN_LOG_MIN_HIGH_WATERMARK 64U

doca_error_t
dmesh_doca_dpa_msgq_drain_pending(struct dmesh_doca_dpa_msgq *msgq,
				  uint32_t budget,
				  uint32_t *submitted)
{
	uint32_t local_submitted = 0;
	doca_error_t result = DOCA_SUCCESS;

	if (msgq == NULL)
		return DOCA_ERROR_INVALID_VALUE;
	if (budget == 0)
		budget = DMESH_DPA_MSGQ_PENDING_SEND_DEPTH;

	while (msgq->pending_count != 0 && local_submitted < budget) {
		struct comch_grpc_serialize_req_msg *entry =
			(struct comch_grpc_serialize_req_msg *)&msgq->pending_sends[msgq->pending_head];

		result = dmesh_doca_dpa_msgq_submit_once(msgq,
							 entry,
							 sizeof(struct comch_grpc_serialize_req_msg));
		if (result != DOCA_SUCCESS) {
			if (result != DOCA_ERROR_AGAIN && result != DOCA_ERROR_NO_MEMORY)
				DOCA_LOG_ERR("Failed to drain pending DPA MsgQ send - %s",
					     doca_error_get_name(result));
			break;
		}

		dmesh_doca_dpa_msgq_pending_pop(msgq);
		local_submitted++;
	}

	if (submitted != NULL)
		*submitted = local_submitted;

#if DEBUG_LOG
	if (local_submitted != 0 && msgq->pending_count == 0 &&
	    msgq->pending_high_watermark > 0) {
		if (msgq->pending_high_watermark >= DMESH_DPA_MSGQ_DRAIN_LOG_MIN_HIGH_WATERMARK)
			DOCA_LOG_INFO("DPA MsgQ pending send queue drained: submitted=%u high_watermark=%u",
				      local_submitted, msgq->pending_high_watermark);
                      
        msgq->pending_high_watermark = 0;
    }
#endif

	return result;
}

/*
 * Send message to DPA using NVMf DOCA DPA MsgQ
 *
 * @msgq [in]: The MsgQ to be used for the send operation
 * @msg [in]: The message to send
 * @msg_size [in]: The message size
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t 
dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size)
{
	doca_error_t result;
    union doca_data user_data;

	struct doca_comch_producer_task_send *send_task;
	result = doca_comch_producer_task_send_alloc_init(msgq->producer,
							  NULL,
							  msg,
							  msg_size,
							  /*consumer_id=*/1,
							  &send_task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
			     doca_error_get_name(result));
		return result;
	}

	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);

    user_data.ptr = msgq;
    doca_task_set_user_data(task, user_data);

    int attempt = 0;
    do {
        result = doca_task_submit(task);
        attempt++;
        if (attempt > 4096 * 4096) {
            DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task after %d attempts - %s",
                         attempt, doca_error_get_name(result));
            doca_task_free(task);
            return result;
        }
    } while (result == DOCA_ERROR_AGAIN);

    if (attempt > 50000) {
        DOCA_LOG_INFO("Submitted send task after %d attempts", attempt);
    }
    
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task - %s",
			     doca_error_get_name(result));
		doca_task_free(task);
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t 
dmesh_doca_dpa_msgq_send_bulk(struct dmesh_doca_dpa_msgq *msgq, uint32_t num_msg,
                                void *msg, uint32_t msg_size)
{
	struct doca_comch_producer_task_send *send_task;
    struct doca_task *task;
	doca_error_t result;
    int i;
    // struct comch_msg *comch_msg = (struct comch_msg *)msg;

    for (i = 0; i < num_msg; i++) {
        result = doca_comch_producer_task_send_alloc_init(msgq->producer,
                                  NULL,
                                  msg,
                                  msg_size,
                                  /*consumer_id=*/1,
                                  &send_task);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
                     doca_error_get_name(result));
            return result;
        }
        task = doca_comch_producer_task_send_as_task(send_task);
        result = doca_task_submit(task);
        if (result != DOCA_SUCCESS) {
            if (result != DOCA_ERROR_AGAIN)
                DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task - %s",
                     doca_error_get_name(result));
            doca_task_free(task);
            return result;
        }
    }
    // DOCA_LOG_INFO("Sent mesg done.");
	return DOCA_SUCCESS;
}

static doca_error_t
setup_dpa_buf_array_common(struct objects *objs, struct doca_buf_arr **buf_arr,
                           size_t num_elem, struct doca_mmap *mmap,
                           size_t elem_size, size_t offset)
{
    doca_error_t result;

    result = doca_buf_arr_create(num_elem, buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create buffer array: %s", doca_error_get_descr(result));
        return result;
    }

    result = doca_buf_arr_set_target_dpa(*buf_arr, objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set buffer array target DPA: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_set_params(*buf_arr, mmap, elem_size, offset);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set buffer array params: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_start(*buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start buffer array: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    return DOCA_SUCCESS;

destroy_buf_arr:
    doca_buf_arr_destroy(*buf_arr);
    *buf_arr = NULL;
    return result;
}

doca_error_t
setup_dpa_buf_array(struct objects *objs, size_t num_elem, struct doca_mmap *mmap)
{
    return setup_dpa_buf_array_common(objs, &objs->buf_arr, num_elem, mmap,
                                      sizeof(struct dma_desc), 0);
}

doca_error_t
setup_dpa_consumer_state_buf_array(struct objects *objs, struct doca_mmap *mmap)
{
    return setup_dpa_buf_array_common(objs, &objs->consumer_state_buf_arr, 1, mmap,
                                      sizeof(struct dma_ring_consumer_state), 0);
}

doca_error_t
setup_dpa_host_mmap_buf_array(struct objects *objs, struct doca_mmap *mmap, size_t mmap_size)
{
    return setup_dpa_buf_array_common(objs, &objs->host_mmap_buf_arr, 1, mmap,
                                      mmap_size, 0);
}

doca_error_t
setup_dpa_dpu_mmap_buf_array(struct objects *objs, struct doca_mmap *mmap, size_t mmap_size)
{
    return setup_dpa_buf_array_common(objs, &objs->dpu_mmap_buf_arr, 1, mmap,
                                      mmap_size, 0);
}
