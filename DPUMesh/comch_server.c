
#include "comch_server.h"

#include <time.h>

#include "common.h"
#include "object.h"
#include "dpa.h"
#include "dpa_common.h"
#include "comch_common.h"
#include "comch_consumer.h"
#include "comch_msgq.h"
#include "dma.h"
#include "ring.h"

#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_log.h>
#include <doca_comch_producer.h>


DOCA_LOG_REGISTER(COMCH_SERVER);

static void server_send_task_completion_callback(struct doca_comch_task_send *task,
						 union doca_data task_user_data,
						 union doca_data ctx_user_data)
{
	struct objects *objs;

	(void)task_user_data;

	objs = (struct objects *)ctx_user_data.ptr;
	(void)objs;
	DOCA_LOG_INFO("Server task sent successfully");
	doca_task_free(doca_comch_task_send_as_task(task));
}

static void server_send_task_completion_err_callback(struct doca_comch_task_send *task,
						     union doca_data task_user_data,
						     union doca_data ctx_user_data)
{
	struct objects *objs;

	(void)task_user_data;

	objs = (struct objects *)ctx_user_data.ptr;
	doca_task_free(doca_comch_task_send_as_task(task));
	(void)doca_ctx_stop(doca_comch_server_as_ctx(objs->cc_server));
}

/**
 * Server sends a message to client
 *
 * @sample_objects [in]: The sample object to use
 * @msg [in]: The msg to send
 * @len [in]: The msg length
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t 
server_send_msg_conn(struct objects *objs, struct doca_comch_connection *connection,
		     const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;

	result = doca_comch_server_task_send_alloc_init(objs->cc_server, connection,
							(void *)msg, len, &task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate server task with error = %s", doca_error_get_name(result));
		return result;
	}

	result = doca_task_submit(doca_comch_task_send_as_task(task));
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send server task with error = %s", doca_error_get_name(result));
		doca_task_free(doca_comch_task_send_as_task(task));
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t
server_send_msg(struct objects *objs, const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;

	result = doca_comch_server_task_send_alloc_init(objs->cc_server, objs->connection,
							(void *)msg, len, &task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate server task with error = %s", doca_error_get_name(result));
		return result;
	}

	result = doca_task_submit(doca_comch_task_send_as_task(task));
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send server task with error = %s", doca_error_get_name(result));
		doca_task_free(doca_comch_task_send_as_task(task));
		return result;
	}

	return DOCA_SUCCESS;
}

/**
 * Callback for server message recv event
 *
 * @event [in]: Recv event object
 * @recv_buffer [in]: Message buffer
 * @msg_len [in]: Message len
 * @comch_connection [in]: Connection the message was received on
 */
static void server_message_recv_callback(struct doca_comch_event_msg_recv *event,
					 uint8_t *recv_buffer,
					 uint32_t msg_len,
					 struct doca_comch_connection *comch_connection)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;
	struct dmesh_comch_msg *comch_msg;

	(void)event;

	// DOCA_LOG_INFO("Message received: '%.*s', size: %u", (int)msg_len, recv_buffer, msg_len);

	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;
	objs->connection = comch_connection;

	comch_msg = (struct dmesh_comch_msg *)recv_buffer;

	DOCA_LOG_INFO("Received message from client with type = %u", comch_msg->type);
	switch (comch_msg->type) {
	case DMESH_MSG_EXPORT_METADATA:
		if (msg_len < sizeof(struct dmesh_export_metadata_msg)) {
			DOCA_LOG_ERR("Received invalid METADATA message from client");
			return;
		}
		{
			struct dmesh_conn *conn = dmesh_conn_get(objs, comch_connection);

			if (conn == NULL) {
				/* Metadata can arrive before the connection event was
				 * processed; bind the slot here. */
				conn = dmesh_conn_open(objs, comch_connection);
			}
			if (conn == NULL) {
				DOCA_LOG_ERR("No connection slot for metadata message");
				return;
			}
			result = process_export_metadata_msg(conn, (struct dmesh_export_metadata_msg *)recv_buffer);
		}
		break;
	default:

		DOCA_LOG_ERR("Received unknown message type from client: %u", comch_msg->type);
		break;
	}
}

static void dmesh_doca_comch_server_conn_ev_cb(struct doca_comch_event_connection_status_changed *event,
					       struct doca_comch_connection *comch_connection,
					       uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	if (change_success == 0) {
		DOCA_LOG_ERR("Failed connection received");
		return;
	}

	(void)event;
	comch_server = doca_comch_server_get_server_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;
	// objs->connection = comch_connection;

	DOCA_LOG_INFO("New connection established with client");
}

/**
 * Callback for disconnection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the disconnection was successful or not
 */
static void dmesh_doca_comch_server_disconn_ev_cb(struct doca_comch_event_connection_status_changed *event,
						struct doca_comch_connection *comch_connection,
						uint8_t change_success)
{
	(void)event;
	(void)comch_connection;

	if (change_success == 0)
		DOCA_LOG_ERR("Failed disconnection received");
}

/**
 * Callback for connection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the connection was successful or not
 */
static void server_connection_event_callback(struct doca_comch_event_connection_status_changed *event,
					     struct doca_comch_connection *comch_connection,
					     uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	if (change_success == 0) {
		DOCA_LOG_ERR("Failed connection received");
		return;
	}

	(void)event;

	comch_server = doca_comch_server_get_server_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;
	objs->connection = comch_connection;

	DOCA_LOG_INFO("New connection established with client");

	/* Bind a connection slot and assign a pre-created DPA thread to it. Both
	 * are pure memory operations, so they are safe inside this event callback.
	 * The pool may not exist yet if the client connects before the first
	 * advance() runs; dmesh_doca_conn_advance retries the assignment then. */
	struct dmesh_conn *conn = dmesh_conn_open(objs, comch_connection);
	if (conn == NULL) {
		DOCA_LOG_ERR("Connection rejected: no free connection slot");
		return;
	}
	conn->dpa_thread = dmesh_dpa_thread_pool_alloc(objs, comch_connection);
	if (conn->dpa_thread == NULL)
		DOCA_LOG_WARN("No DPA thread assigned to new connection yet");
}

/**
 * Callback for disconnection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the disconnection was successful or not
 */
static void server_disconnection_event_callback(struct doca_comch_event_connection_status_changed *event,
						struct doca_comch_connection *comch_connection,
						uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	(void)event;

	if (change_success == 0)
		DOCA_LOG_ERR("Failed disconnection received");

	/* Return the DPA thread owned by this connection to the pool */
	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	if (comch_server == NULL)
		return;
	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS || user_data.ptr == NULL)
		return;

	objs = (struct objects *)user_data.ptr;

	/* Do NOT tear down DOCA resources here: this callback runs inside
	 * doca_pe_progress(control PE), and teardown must progress the consumer PE
	 * (re-entrancy is forbidden). Just mark the slot; dmesh_doca_ctrl_advance
	 * performs the teardown from the driver loop. */
	{
		struct dmesh_conn *conn = dmesh_conn_get(objs, comch_connection);

		if (conn != NULL && conn->state != DMESH_CONN_FREE)
			conn->state = DMESH_CONN_CLOSING;
	}
}

doca_error_t
dmesh_doca_init_comch_server(struct dmesh_doca_objects *objs, const char *server_name, 
							 bool enable_fast_path)
{
	doca_error_t result;
	struct doca_ctx *ctx;
	union doca_data user_data;
	uint32_t max_msg_size, max_rq_size;

	/* create DOCA comch server */
	result = doca_comch_server_create(objs->dev, objs->rep_dev,
				server_name, &objs->cc_server);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create server with error = %s", doca_error_get_name(result));
		return result;
	}

	ctx = doca_comch_server_as_ctx(objs->cc_server);

	/* connect the ctx to the PE */
    result = doca_pe_connect_ctx(objs->pe, ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding pe context to server with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* configure send tasks completion Callbacks */
	result = doca_comch_server_task_send_set_conf(objs->cc_server,
			server_send_task_completion_callback,
			server_send_task_completion_err_callback,
			CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* configure recv callback */
	result = doca_comch_server_event_msg_recv_register(objs->cc_server, server_message_recv_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* configure connection event callback */
	result = doca_comch_server_event_connection_status_changed_register(objs->cc_server,
									dmesh_doca_comch_server_conn_ev_cb,
									dmesh_doca_comch_server_disconn_ev_cb);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding connection status changed event cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }      

	/* Config the data_path related events */
	if (enable_fast_path) {
		result = doca_comch_server_event_consumer_register(objs->cc_server,
									dmesh_doca_server_new_consumer_cb,
									expired_consumer_callback);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_server;
		}
	}

	/* set max message and recv queue sizes as device-supported max size */
	result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(objs->dev), &max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    } 

    result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(objs->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }
    
    result = doca_comch_server_set_max_msg_size(objs->cc_server, max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_set_recv_queue_size(objs->cc_server, max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* set user data for the ctx */
	user_data.ptr = (void *)objs;
	result = doca_ctx_set_user_data(ctx, user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
		goto destroy_server;
	}

	/* start the comch server ctx */
	result = doca_ctx_start(ctx);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start server context with error = %s", doca_error_get_name(result));
		goto destroy_server;
	}

	return result;

destroy_server:
	doca_comch_server_destroy(objs->cc_server);
	objs->cc_server = NULL;
	return result;
}

doca_error_t
start_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path)
{
    doca_error_t result;
    struct doca_ctx *ctx;
    union doca_data udata;
    uint32_t max_msg_size, max_rq_size;

	/* create a progress engine */
	if (!objs->pe) {
		result = doca_pe_create(&(objs->pe));
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
			return result;
		}
	}

	/* The event-driven driver arms this PE (doca_pe_request_notification). In the
	 * default SELECTIVE event mode an armed PE only progresses contexts that
	 * received an event, which stalls outbound work such as the consumer
	 * registration handshake. PROGRESS_ALL keeps doca_pe_progress unconditional. */
	result = doca_pe_set_event_mode(objs->pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set PE event mode: %s", doca_error_get_name(result));
		goto destroy_pe;
	}
	
    result = doca_comch_server_create(objs->dev, objs->rep_dev,
                server_name, &objs->cc_server);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create server with error = %s", doca_error_get_name(result));
        goto destroy_pe;
    }

    ctx = doca_comch_server_as_ctx(objs->cc_server);

    result = doca_pe_connect_ctx(objs->pe, ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding pe context to server with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_task_send_set_conf(objs->cc_server,
                server_send_task_completion_callback,
                server_send_task_completion_err_callback,
                CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_event_msg_recv_register(objs->cc_server, server_message_recv_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_event_connection_status_changed_register(objs->cc_server,
                                        server_connection_event_callback,
                                        server_disconnection_event_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding connection status changed event cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }                                        

    /* Config the data_path related events */
	if (is_fast_path) {
		result = doca_comch_server_event_consumer_register(objs->cc_server,
									server_new_consumer_callback,
									expired_consumer_callback);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_server;
		}
	}

    result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(objs->dev), &max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    } 

    result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(objs->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }
    
    result = doca_comch_server_set_max_msg_size(objs->cc_server, max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_set_recv_queue_size(objs->cc_server, CC_RECV_QUEUE_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    udata.ptr = (void *)objs;
    result = doca_ctx_set_user_data(ctx, udata);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_ctx_start(ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start server context with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* Server ctx is live but not yet connected. The connection is established
	 * later, event-driven, by progressing objs->pe (see dmesh_doca_ctrl_advance). */
	objs->phase = DMESH_DOCA_STATE_SERVER_STARTED;

    return DOCA_SUCCESS;

destroy_server:
    doca_comch_server_destroy(objs->cc_server);
    objs->cc_server = NULL;
destroy_pe:
    doca_pe_destroy(objs->pe);
    objs->pe = NULL;
    return result;
}

/*
 * Baseline (busy-poll) control-path server init: start the server, then spin on
 * the control PE until the host connects. Preserved for the original
 * run_dpu_worker() path; new event-driven code uses start_comch_ctrl_path_server
 * plus the dmesh_doca_ctrl_* helpers instead.
 */
doca_error_t
init_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path)
{
    doca_error_t result;
	struct timespec ts = {
		.tv_nsec = SLEEP_IN_NANOS,
	};

	result = start_comch_ctrl_path_server(server_name, objs, is_fast_path);
	if (result != DOCA_SUCCESS)
		return result;

	while (objs->connection == NULL) {
		if (doca_pe_progress(objs->pe) == 0)
			nanosleep(&ts, &ts);
	}

	DOCA_LOG_INFO("Server connection established");

    return DOCA_SUCCESS;
}

doca_error_t
export_dpa_comp_to_host(struct objects *objs)
{
	doca_error_t result;
	struct dmesh_dpa_comp_msg dpa_comp_msg;
	dpa_comp_msg.type = DMESH_MSG_EXPORT_DPA_COMP;

	result = doca_comch_consumer_completion_get_dpa_handle(objs->dpa_comch->consumer_comp,
									&dpa_comp_msg.dpa_consumer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA consumer completion handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_dpa_completion_get_dpa_handle(objs->dpa_comch->producer_comp,
									&dpa_comp_msg.dpa_producer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA producer completion handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_comch_producer_get_dpa_handle(objs->dpa_comch->recv.producer,
									&dpa_comp_msg.dpa_producer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA producer handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_comch_consumer_get_dpa_handle(objs->dpa_comch->send.consumer,
									&dpa_comp_msg.dpa_consumer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA consumer handle - %s",
				doca_error_get_name(result));
		return result;
	}

	DOCA_LOG_INFO("dpa_consumer_comp: 0x%lx, dpa_producer_comp: 0x%lx, dpa_producer: 0x%lx, dpa_consumer: 0x%lx",
			dpa_comp_msg.dpa_consumer_comp,
			dpa_comp_msg.dpa_producer_comp,
			dpa_comp_msg.dpa_producer,
			dpa_comp_msg.dpa_consumer);

	return server_send_msg(objs, (const char *)&dpa_comp_msg, sizeof(dpa_comp_msg));
}

/*
 * ---------------------------------------------------------------------------
 * Event-driven (on-demand) control-path helpers.
 *
 * These replace the busy-poll loops that waited on the control PE (objs->pe).
 * A caller (the C test driver in dpu_worker.c, or later the Rust AsyncFd loop)
 * uses them as: get_fd once, then repeatedly arm -> wait on fd -> clear_and_drain
 * -> advance, until the state machine reaches DMESH_DOCA_STATE_RUNNING.
 * ---------------------------------------------------------------------------
 */

doca_error_t
dmesh_doca_ctrl_get_fd(struct objects *objs, int *out_fd)
{
	doca_error_t result;
	doca_notification_handle_t handle;

	if (objs == NULL || objs->pe == NULL || out_fd == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	result = doca_pe_get_notification_handle(objs->pe, &handle);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get control PE notification handle: %s", doca_error_get_name(result));
		return result;
	}

	*out_fd = (int)handle;
	return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_ctrl_arm(struct objects *objs)
{
	if (objs == NULL || objs->pe == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	return doca_pe_request_notification(objs->pe);
}

doca_error_t
dmesh_doca_ctrl_drain(struct objects *objs)
{
	if (objs == NULL || objs->pe == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	while (doca_pe_progress(objs->pe) != 0)
		;

	return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_ctrl_clear_and_drain(struct objects *objs, int fd)
{
	doca_error_t result;

	if (objs == NULL || objs->pe == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	result = doca_pe_clear_notification(objs->pe, (doca_notification_handle_t)fd);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to clear control PE notification: %s", doca_error_get_name(result));
		return result;
	}

	/* Drain until no more progress: mandatory before re-arming, otherwise the
	 * fd may not signal again (no new edge) and the waiter would deadlock. */
	while (doca_pe_progress(objs->pe) != 0)
		;

	return DOCA_SUCCESS;
}

/* Tear down all DOCA resources a connection acquired, so its slot (and its DPA
 * pool thread) can be reused without restarting the proxy. Runs from advance()
 * - never inside the disconnect callback - so progressing PEs here is safe. */
static void
dmesh_conn_teardown(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;

    DOCA_LOG_INFO("Tearing down connection slot %ld", conn - objs->conns);

    /* Quiesce the DPA thread first: signal its poll loop to exit, wait for the
     * ack, then stop it. Its DPA-side msgq/completion ctxs can only idle (and
     * later be destroyed) once the thread is no longer hot-looping. */
    dmesh_doca_dpa_thread_quiesce(conn->dpa_thread);

    /* Datapath consumer (DPU side, on the shared consumer PE). */
    if (conn->consumer != NULL) {
        enum doca_ctx_states st;
        int spins = 0;
        struct doca_ctx *cctx = doca_comch_consumer_as_ctx(conn->consumer);

        if (doca_ctx_get_state(cctx, &st) == DOCA_SUCCESS && st != DOCA_CTX_STATE_IDLE) {
            (void)doca_ctx_stop(cctx);
            while (spins++ < 100000 &&
                   doca_ctx_get_state(cctx, &st) == DOCA_SUCCESS && st != DOCA_CTX_STATE_IDLE)
                doca_pe_progress(objs->consumer_pe);
        }
        (void)doca_comch_consumer_destroy(conn->consumer);
        conn->consumer = NULL;
    }
    if (conn->consumer_mem != NULL) {
        clean_local_mem_bufs(conn->consumer_mem);
        free(conn->consumer_mem);
        conn->consumer_mem = NULL;
    }

    /* DPA comch (both MsgQs + completions) and the DPA thread itself. */
    dmesh_doca_dpa_comch_destroy(conn);
    dmesh_doca_dpa_thread_destroy(conn->dpa_thread);

    /* DPA buffer array over the DMA ring. */
    if (conn->buf_arr != NULL) {
        (void)doca_buf_arr_stop(conn->buf_arr);
        (void)doca_buf_arr_destroy(conn->buf_arr);
        conn->buf_arr = NULL;
    }

    /* Remote mmaps imported from the host's metadata message. */
    if (conn->ring_mmap != NULL) {
        (void)doca_mmap_destroy(conn->ring_mmap);
        conn->ring_mmap = NULL;
    }
    if (conn->sndbuf.mmap != NULL) {
        (void)doca_mmap_destroy(conn->sndbuf.mmap);
        conn->sndbuf.mmap = NULL;
    }
    if (conn->rcvbuf.mmap != NULL) {
        (void)doca_mmap_destroy(conn->rcvbuf.mmap);
        conn->rcvbuf.mmap = NULL;
    }
    conn->sndbuf.buf = NULL;
    conn->rcvbuf.buf = NULL;

    /* Local staging buffer + its mmap. */
    if (conn->local_mmap != NULL) {
        destroy_mmap_and_free_buffer(conn->local_mmap, conn->dma_buffer);
        conn->local_mmap = NULL;
        conn->dma_buffer = NULL;
    }

    /* Reverse (response) path resources owned by the DPU. */
    if (conn->rcv_ring != NULL) {
        free_dma_ring(conn->rcv_ring);
        conn->rcv_ring = NULL;
    }
    if (conn->tx_staging_mmap != NULL) {
        destroy_mmap_and_free_buffer(conn->tx_staging_mmap, conn->tx_staging);
        conn->tx_staging_mmap = NULL;
        conn->tx_staging = NULL;
    }
    conn->tx_staging_len = 0;
    conn->tx_pos = 0;
    conn->reverse_exported = false;

    /* Per-connection DMA engine (ctx, inventory, task pool, recv/pending rings). */
    cleanup_dma_tasks(conn);

    /* Return the DPA pool thread and unbind the slot. */
    dmesh_dpa_thread_pool_release(objs, conn->connection);

    /* Reset the slot for reuse. */
    {
        struct dmesh_flow_id flow = conn->flow;
        (void)flow;
        conn->connection = NULL;
        conn->remote_consumer_id = 0;
        conn->dpa_thread = NULL;
        conn->state = DMESH_CONN_FREE;
        memset(&conn->flow, 0, sizeof(conn->flow));
    }
}

/* Advance one connection's setup state machine. A failure parks only this
 * connection (DMESH_CONN_ERROR); other connections keep running. */
static void
dmesh_doca_conn_advance(struct dmesh_conn *conn)
{
	struct objects *objs = conn->objs;
	doca_error_t result;

	switch (conn->state) {
	case DMESH_CONN_NEW:
		/* The connection callback normally assigns a pool thread; retry here
		 * in case the client connected before the pool existed. */
		if (conn->dpa_thread == NULL)
			conn->dpa_thread = dmesh_dpa_thread_pool_alloc(objs, conn->connection);
		if (conn->dpa_thread == NULL) {
			DOCA_LOG_ERR("No DPA thread available for connection %p", (void *)conn->connection);
			goto error;
		}

		DOCA_LOG_INFO("Setting up DMA engine, datapath consumer and DPA msgq for connection %p",
			      (void *)conn->connection);

		/* private DMA engine (own QP + task pool) for this connection */
		result = init_dma_tasks(conn, DMA_TASKS_PER_CONN);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init DMA tasks: %s", doca_error_get_name(result));
			goto error;
		}

		result = init_comch_datapath_consumer(conn);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init datapath consumer: %s", doca_error_get_name(result));
			goto error;
		}

		result = init_comch_dpa_msgq(conn, objs->consumer_pe);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init comch DPA msgq: %s", doca_error_get_name(result));
			goto error;
		}

		conn->state = DMESH_CONN_CONSUMER_STARTING;
		break;

	case DMESH_CONN_CONSUMER_STARTING: {
		/* The consumer registration handshake with the peer is asynchronous;
		 * poll its ctx state (progressed by the driver's consumer-PE drain).
		 * If the peer died mid-handshake the ctx falls back to IDLE - park
		 * the slot instead of waiting forever. */
		enum doca_ctx_states cstate;

		if (conn->consumer == NULL ||
		    doca_ctx_get_state(doca_comch_consumer_as_ctx(conn->consumer), &cstate) != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Consumer vanished during startup for connection %p",
				     (void *)conn->connection);
			goto error;
		}

		if (cstate == DOCA_CTX_STATE_RUNNING) {
			DOCA_LOG_INFO("Consumer running for connection %p", (void *)conn->connection);
			conn->state = DMESH_CONN_AWAIT_METADATA;
		} else if (cstate == DOCA_CTX_STATE_IDLE) {
			DOCA_LOG_ERR("Consumer registration failed for connection %p (peer gone?)",
				     (void *)conn->connection);
			goto error;
		}
		/* STARTING/STOPPING: not ready yet; retry on the next advance */
		break;
	}

	case DMESH_CONN_AWAIT_METADATA:
		/* All three remote mmaps arrive in a single metadata message
		 * (process_export_metadata_msg), so they become ready together. */
		if (conn->ring_mmap == NULL || conn->sndbuf.mmap == NULL || conn->rcvbuf.mmap == NULL)
			break; /* not ready yet: awaiting this host's metadata export */

		DOCA_LOG_INFO("Received remote DMA metadata; completing DPA setup for connection %p",
			      (void *)conn->connection);

		result = setup_dpa_buf_array(conn, DMA_RING_SIZE, conn->ring_mmap);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to setup DPA buffer array: %s", doca_error_get_name(result));
			goto error;
		}

		result = alloc_buffer_and_set_mmap(&conn->local_mmap, objs->dev,
						   &conn->dma_buffer, BUFFER_SIZE,
						   DOCA_ACCESS_FLAG_PCI_READ_WRITE);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to allocate DMA buffer: %s", doca_error_get_name(result));
			goto error;
		}

		result = dmesh_doca_run_dpa_thread(conn);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_name(result));
			goto error;
		}

		result = send_dma_request_to_dpa(conn);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_name(result));
			goto error;
		}

		conn->state = DMESH_CONN_RUNNING;
		DOCA_LOG_INFO("Connection %p is running", (void *)conn->connection);
		/* fall through to export the reverse path metadata this same tick */
		/* fallthrough */

	case DMESH_CONN_RUNNING:
		/* Reverse (response) path: allocate this connection's rcv_ring +
		 * tx_staging and hand their export descriptors to the host, whose
		 * DPA thread then DMAs responses back into the host rcvbuf. Idempotent
		 * (guarded by conn->reverse_exported); retried until it succeeds. */
		if (!conn->reverse_exported) {
			result = export_rcv_ring_metadata(conn);
			if (result != DOCA_SUCCESS)
				DOCA_LOG_ERR("Failed to export reverse metadata (will retry): %s",
					     doca_error_get_name(result));
		}
		break;

	case DMESH_CONN_CLOSING:
		/* Host disconnected (or setup failed): release everything so the slot
		 * and its DPA pool thread can be reused. Sets state to FREE. */
		dmesh_conn_teardown(conn);
		break;

	case DMESH_CONN_FREE:
	case DMESH_CONN_ERROR:
	default:
		break;
	}
	return;

error:
	conn->state = DMESH_CONN_ERROR;
}

doca_error_t
dmesh_doca_ctrl_advance(struct objects *objs, enum dmesh_doca_init_state *out_state)
{
	doca_error_t result;
	int i;

	if (objs == NULL || out_state == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	/* One-time shared infrastructure: DPA instance, thread pool, the shared
	 * consumer PE and the DMA engine. None of it depends on a connection. */
	if (objs->phase == DMESH_DOCA_STATE_SERVER_STARTED) {
		DOCA_LOG_INFO("Creating DPA objects, thread pool, consumer PE and DMA engine");

		result = init_dpa_objects(objs);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init DPA objects: %s", doca_error_get_name(result));
			goto error;
		}

		result = dmesh_dpa_thread_pool_init(objs);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init DPA thread pool: %s", doca_error_get_name(result));
			goto error;
		}

		result = doca_pe_create(&objs->consumer_pe);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to create consumer PE: %s", doca_error_get_name(result));
			goto error;
		}
		result = doca_pe_set_event_mode(objs->consumer_pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to set consumer PE event mode: %s", doca_error_get_name(result));
			goto error;
		}

		/* DMA engines are per connection now (created in DMESH_CONN_NEW) */

		objs->phase = DMESH_DOCA_STATE_RUNNING;
	}

	/* Serve every bound connection; each has its own state machine. */
	for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
		if (objs->conns[i].state != DMESH_CONN_FREE)
			dmesh_doca_conn_advance(&objs->conns[i]);
	}

	*out_state = objs->phase;
	return DOCA_SUCCESS;

error:
	objs->phase = DMESH_DOCA_STATE_ERROR;
	*out_state = DMESH_DOCA_STATE_ERROR;
	return result;
}
