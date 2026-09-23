#include "wire_push.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <doca_buf_array.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_pe.h>

#include "buffer.h"
#include "comch_client.h"
#include "comch_common.h"
#include "comch_msgq.h"
#include "comch_producer.h"
#include "common.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "object.h"
#include "ring.h"

/*
 * DPUMesh transport, host end. Uses the unmodified DPUMesh host sources: Comch
 * client and producer setup, the forward descriptor ring, buffer registration
 * and the export messages.
 *
 * Push wire: the DPU's DMA engine lands reverse batches in the window's slot
 * ring + data ring; the host polls the slots and publishes a consumption
 * cursor the DPU pulls.
 *
 * Pull wire (DPUMESH_WIRE=pull): the reverse path mirrors the forward one. Per
 * connection the DPU exports its descriptor ring (rcv_ring) and tx_staging
 * (EXPORT_RCV_RING); the host imports both on the DPA device, runs one DPA
 * thread with the same poll_desc_ring kernel the DPU uses, and that thread
 * copies tx_staging into the window's data area, delivering one fused msgq
 * completion per descriptor (dpa.c's recv callback queues it in recv_segs).
 * The application's releases feed the kernel's staging gate (rd_pos) so it
 * never overwrites bytes the host still holds.
 */

DOCA_LOG_REGISTER(WIRE);

/* dpacc host stubs (dpa_kernel.a) */
extern doca_dpa_func_t thread_init_rpc;
extern struct doca_dpa_app *DPU_mesh_dpa_app;

_Static_assert(WIRE_PUSH_DESC_N == DMESH_PUSH_DESC_N, "push slot count");
_Static_assert(WIRE_PUSH_DATA_OFF == DMESH_PUSH_DATA_OFF, "push data offset");
_Static_assert(WIRE_PUSH_WINDOW == BUFFER_SIZE, "push window");
_Static_assert(WIRE_MODE_CLIENT_PULL == DMESH_FLOW_MODE_CLIENT, "client mode");
_Static_assert(WIRE_MODE_BACKEND == DMESH_FLOW_MODE_BACKEND, "backend mode");
_Static_assert(WIRE_MODE_INGRESS_PUSH == DMESH_FLOW_MODE_INGRESS_PUSH, "ingress push mode");
_Static_assert(WIRE_MODE_BACKEND_PULL == DMESH_FLOW_MODE_BACKEND_PULL, "backend pull mode");

#define WIRE_RING_SIZE 1024u            /* Forward ring depth of the host library */
#define WIRE_REV_READY_MS 5000          /* Wait for the DPU's EXPORT_RCV_RING */
#define WIRE_RD_POS_BATCH (64u * 1024u) /* Pull: bytes released between rd_pos publications */
#define WIRE_DEFAULT_REV_PCI "0b:00.0"  /* Pull: the host PF that runs the DPA process */
#define WIRE_CTX_STOP_SPINS 100000      /* Bound on progressing a stopping ctx to IDLE */

struct wire_dev {
	struct doca_dev *dev;               /* Comch / forward device */
	int pull;                           /* Nonzero: the pull wire */
	struct objects *rev;                /* Pull: the DPA device (rev->dev, rev->dpa_pool->dpa) */
	struct dmesh_dpa_thread_pool *rev_pool;
	struct doca_dev *base_dev;          /* Pull: the PF that hosts the DPA process */
	struct doca_dpa *base_dpa;          /* == rev_pool->dpa unless extended to an SF */
};

struct wire_mem {
	struct doca_mmap *mmap;
	void *buf;
	size_t bytes;
	doca_dpa_dev_mmap_t dpa;            /* Handle on the forward device (the DPU DPA reads the TX pool) */
	doca_dpa_dev_mmap_t dpa_rev;        /* Pull: handle on the DPA device (the host DPA writes the RX region) */
};

struct wire_conn {
	struct objects *objs;
	struct wire_dev *dev;
	doca_dpa_dev_mmap_t tx_dpa;
	volatile struct dmesh_push_desc *descs;
	volatile struct dmesh_push_cursor *cursor;
	size_t data_size;
	uint64_t expected;                  /* Push: next batch sequence */
	int closed;
	/* pull */
	struct dmesh_conn *rc;              /* The reverse connection on the DPA device */
	struct objects *ro;                 /* rc's objects: the shared DPA device + this connection's own PE
	                                     * (a progress engine is single-owner; each slot polls its own) */
	struct doca_mmap *tx_mmap;          /* Imported DPU tx_staging */
	struct doca_mmap *ring_mmap;        /* Imported DPU rcv_ring */
	uint64_t rx_seq;                    /* Segments delivered to the carrier */
	uint64_t consumed_seq;              /* Segments the carrier released */
	uint32_t seg_end[WIRE_PUSH_DESC_N]; /* End offset of delivered segment seq % N */
	uint32_t rd_pos;                    /* Kernel read watermark last published */
	uint64_t rd_published_bytes;
	uint64_t rd_published_seq;
};

/**
 * Map a DOCA error to the errno the carrier reports
 *
 * @result [in]: DOCA error
 * @return: errno value
 */
static int error_number(doca_error_t result)
{
	switch (result) {
	case DOCA_ERROR_NO_MEMORY:
		return ENOMEM;
	case DOCA_ERROR_INVALID_VALUE:
		return EINVAL;
	case DOCA_ERROR_AGAIN:
		return EAGAIN;
	case DOCA_ERROR_NOT_FOUND:
		return ENODEV;
	default:
		return EIO;
	}
}

/**
 * Register the DOCA log backends once per process
 *
 * DPUMESH_SDK_LOG=debug|info|warning surfaces the SDK's own reasons for a
 * failing DOCA call (silent otherwise).
 */
static void logging_once(void)
{
	static int logging;
	const char *level;
	struct doca_log_backend *sdk = NULL;

	if (logging)
		return;
	logging = 1;

	(void)doca_log_backend_create_standard();

	level = getenv("DPUMESH_SDK_LOG");
	if (level == NULL || *level == '\0')
		return;
	if (doca_log_backend_create_with_file_sdk(stderr, &sdk) != DOCA_SUCCESS)
		return;
	if (strcmp(level, "debug") == 0)
		(void)doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_DEBUG);
	else if (strcmp(level, "info") == 0)
		(void)doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_INFO);
	else
		(void)doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_WARNING);
}

/**
 * Progress a stopping ctx on its PE until it is IDLE (bounded)
 *
 * @ctx [in]: Context that was told to stop
 * @pe [in]: Progress engine the context is connected to
 */
static void wait_ctx_idle(struct doca_ctx *ctx, struct doca_pe *pe)
{
	enum doca_ctx_states state;
	int spins = 0;

	while (spins++ < WIRE_CTX_STOP_SPINS &&
	       doca_ctx_get_state(ctx, &state) == DOCA_SUCCESS &&
	       state != DOCA_CTX_STATE_IDLE)
		(void)doca_pe_progress(pe);
}

int wire_dev_pull(const struct wire_dev *dev)
{
	return dev != NULL && dev->pull;
}

/*
 * ---------------------------------------------------------------------------
 * Device
 * ---------------------------------------------------------------------------
 */

/**
 * Bring up the DPA process for the pull wire
 *
 * The DPA runs on a different host function than Comch (the DPU worker's DPA
 * counts against the Comch function); that PF's vhca must be in a DPA EU
 * partition. Several processes may each create their own DPA process on it.
 *
 * With DPUMESH_REV_DEV the process is extended to an SF (doca_dpa_device_extend);
 * the SF then owns every DPA object and the kernel switches to it with
 * doca_dpa_dev_device_set (thread arg dpa_dev, also in the init RPC). On this
 * node's firmware processes may share one SF, but a second distinct SF extended
 * at the same time fails its consumer-completion CQ (devx syndrome 0x5ecb3).
 *
 * @dev [in]: Wire device being opened
 * @pci [in]: Comch PCI address (for the log line)
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t open_pull_dpa(struct wire_dev *dev, const char *pci)
{
	const char *rev_pci = getenv("DPUMESH_REV_PCI");
	const char *rev_dev = getenv("DPUMESH_REV_DEV");
	doca_error_t result;

	if (rev_pci == NULL || *rev_pci == '\0')
		rev_pci = WIRE_DEFAULT_REV_PCI;

	dev->rev = calloc(1, sizeof(*dev->rev));
	dev->rev_pool = calloc(1, sizeof(*dev->rev_pool));
	if (dev->rev == NULL || dev->rev_pool == NULL)
		return DOCA_ERROR_NO_MEMORY;
	dev->rev->dpa_pool = dev->rev_pool;

	result = open_doca_device_with_pci(rev_pci, NULL, &dev->base_dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to open DPA device %s with error = %s", rev_pci, doca_error_get_name(result));
		return result;
	}
	result = doca_dpa_create(dev->base_dev, &dev->base_dpa);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_set_app(dev->base_dpa, DPU_mesh_dpa_app);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_start(dev->base_dpa);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start the DPA process on %s with error = %s (EU partition for its vhca? DPUMESH_REV_PCI?)",
			     rev_pci, doca_error_get_name(result));
		return result;
	}

	if (rev_dev == NULL || *rev_dev == '\0') {
		dev->rev->dev = dev->base_dev;
		dev->rev_pool->dpa = dev->base_dpa;
		DOCA_LOG_INFO("pull wire: host DPA on %s, Comch on %s", rev_pci, pci);
		return DOCA_SUCCESS;
	}

	result = open_doca_device_with_ibdev_name((const uint8_t *)rev_dev, strlen(rev_dev), NULL, &dev->rev->dev);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_device_extend(dev->base_dpa, dev->rev->dev, &dev->rev_pool->dpa);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to extend the DPA process on %s to %s with error = %s", rev_pci, rev_dev,
			     doca_error_get_name(result));
		return result;
	}
	DOCA_LOG_INFO("pull wire: DPA process on %s extended to %s, Comch on %s", rev_pci, rev_dev, pci);
	return DOCA_SUCCESS;
}

int wire_dev_open(const char *pci, struct wire_dev **out)
{
	struct wire_dev *dev;
	const char *wire;
	doca_error_t result;
	int saved;

	logging_once();

	dev = calloc(1, sizeof(*dev));
	if (dev == NULL)
		return -1;

	result = open_doca_device_with_pci(pci, NULL, &dev->dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to open Comch device %s with error = %s", pci, doca_error_get_name(result));
		goto fail;
	}

	wire = getenv("DPUMESH_WIRE");
	dev->pull = wire != NULL && strcmp(wire, "pull") == 0;
	if (dev->pull) {
		result = open_pull_dpa(dev, pci);
		if (result != DOCA_SUCCESS)
			goto fail;
	}

	*out = dev;
	return 0;

fail:
	saved = error_number(result);
	wire_dev_close(dev);
	errno = saved;
	return -1;
}

void wire_dev_close(struct wire_dev *dev)
{
	if (dev == NULL)
		return;

	if (dev->rev != NULL) {
		/* an extended context goes before its base */
		if (dev->rev_pool != NULL && dev->rev_pool->dpa != NULL && dev->rev_pool->dpa != dev->base_dpa)
			(void)doca_dpa_destroy(dev->rev_pool->dpa);
		if (dev->base_dpa != NULL)
			(void)doca_dpa_destroy(dev->base_dpa);
		if (dev->rev->dev != NULL && dev->rev->dev != dev->base_dev)
			(void)doca_dev_close(dev->rev->dev);
		if (dev->base_dev != NULL)
			(void)doca_dev_close(dev->base_dev);
		free(dev->rev_pool);
		free(dev->rev->dpa_comch);
		free(dev->rev);
	}
	if (dev->dev != NULL)
		(void)doca_dev_close(dev->dev);
	free(dev);
}

/*
 * ---------------------------------------------------------------------------
 * Registered memory
 * ---------------------------------------------------------------------------
 */

/**
 * Allocate a buffer and register it with one or two devices
 *
 * Like alloc_buffer_and_set_mmap, with the DPA device added as a second device
 * on the pull wire (the host DPA writes the RX region; the TX pool stays on
 * the forward device only).
 *
 * @mmap [out]: Created mmap
 * @dev [in]: Forward (Comch) device
 * @dev2 [in]: DPA device, or NULL
 * @buffer [out]: Allocated buffer
 * @bytes [in]: Buffer size
 * @access [in]: mmap permissions
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t alloc_buffer_and_set_mmap2(struct doca_mmap **mmap, struct doca_dev *dev, struct doca_dev *dev2,
					       void **buffer, size_t bytes, uint32_t access)
{
	const char *step = "create";
	doca_error_t result;

	result = doca_mmap_create(mmap);
	if (result != DOCA_SUCCESS)
		goto out;

	if (dev2 != NULL) {
		step = "set_max_num_devices";
		result = doca_mmap_set_max_num_devices(*mmap, 2);
		if (result != DOCA_SUCCESS)
			goto destroy_mmap;
	}
	step = "add_dev";
	result = doca_mmap_add_dev(*mmap, dev);
	if (result != DOCA_SUCCESS)
		goto destroy_mmap;
	if (dev2 != NULL) {
		step = "add_dev(dpa)";
		result = doca_mmap_add_dev(*mmap, dev2);
		if (result != DOCA_SUCCESS)
			goto destroy_mmap;
	}
	step = "set_permissions";
	result = doca_mmap_set_permissions(*mmap, access);
	if (result != DOCA_SUCCESS)
		goto destroy_mmap;

	step = "posix_memalign";
	if (posix_memalign(buffer, 4096, bytes) != 0) {
		result = DOCA_ERROR_NO_MEMORY;
		goto destroy_mmap;
	}
	memset(*buffer, 0, bytes);

	step = "set_memrange";
	result = doca_mmap_set_memrange(*mmap, *buffer, bytes);
	if (result != DOCA_SUCCESS)
		goto free_buffer;
	step = "start";
	result = doca_mmap_start(*mmap);
	if (result != DOCA_SUCCESS)
		goto free_buffer;

	return DOCA_SUCCESS;

free_buffer:
	free(*buffer);
	*buffer = NULL;
destroy_mmap:
	(void)doca_mmap_destroy(*mmap);
	*mmap = NULL;
out:
	DOCA_LOG_ERR("Failed to register %zu bytes at %s with error = %s", bytes, step, doca_error_get_name(result));
	return result;
}

int wire_mem_alloc(struct wire_dev *dev, size_t bytes, struct wire_mem **out)
{
	struct wire_mem *mem;
	struct doca_dev *dev2 = dev->pull ? dev->rev->dev : NULL;
	doca_error_t result;

	mem = calloc(1, sizeof(*mem));
	if (mem == NULL)
		return -1;

	result = alloc_buffer_and_set_mmap2(&mem->mmap, dev->dev, dev2, &mem->buf, bytes,
					    DOCA_ACCESS_FLAG_PCI_READ_WRITE | DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
	if (result != DOCA_SUCCESS)
		goto free_mem;

	result = doca_mmap_dev_get_dpa_handle(mem->mmap, dev->dev, &mem->dpa);
	if (result == DOCA_SUCCESS && dev2 != NULL)
		result = doca_mmap_dev_get_dpa_handle(mem->mmap, dev2, &mem->dpa_rev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA handle of the region with error = %s", doca_error_get_name(result));
		goto free_buffer;
	}

	mem->bytes = bytes;
	*out = mem;
	return 0;

free_buffer:
	(void)destroy_mmap_and_free_buffer(mem->mmap, mem->buf);
free_mem:
	free(mem);
	errno = error_number(result);
	return -1;
}

void *wire_mem_base(const struct wire_mem *mem)
{
	return mem != NULL ? mem->buf : NULL;
}

void wire_mem_free(struct wire_mem *mem)
{
	if (mem == NULL)
		return;
	if (mem->mmap != NULL)
		(void)destroy_mmap_and_free_buffer(mem->mmap, mem->buf);
	free(mem);
}

/*
 * ---------------------------------------------------------------------------
 * Pull wire: the reverse DPA thread of one connection
 * ---------------------------------------------------------------------------
 */

/**
 * Release the reverse DPA thread and everything bound to it
 *
 * Order: stop/stopped handshake (the poll loop calls thread_finish), then the
 * msgq/completion contexts, then the thread; never doca_dpa_thread_stop on a
 * comch-attached thread (see dpa.c). The DPU frees tx_staging only after the
 * Comch disconnect that follows, so no copy can target freed memory.
 *
 * @conn [in]: Connection being torn down
 */
static void pull_teardown(struct wire_conn *conn)
{
	struct dmesh_conn *rc = conn->rc;

	if (rc != NULL) {
		dmesh_doca_dpa_thread_quiesce(rc->dpa_thread);
		dmesh_doca_dpa_comch_destroy(rc);
		if (rc->buf_arr != NULL) {
			(void)doca_buf_arr_destroy(rc->buf_arr);
			rc->buf_arr = NULL;
		}
		if (rc->dpa_thread != NULL) {
			dmesh_doca_dpa_thread_destroy(rc->dpa_thread);
			free(rc->dpa_thread);
		}
		free(rc->recv_segs);
		free(rc);
		conn->rc = NULL;
	}
	if (conn->ro != NULL) {
		if (conn->ro->consumer_pe != NULL)
			(void)doca_pe_destroy(conn->ro->consumer_pe);
		free(conn->ro);
		conn->ro = NULL;
	}
	if (conn->tx_mmap != NULL) {
		(void)doca_mmap_stop(conn->tx_mmap);
		(void)doca_mmap_destroy(conn->tx_mmap);
		conn->tx_mmap = NULL;
	}
	if (conn->ring_mmap != NULL) {
		(void)doca_mmap_stop(conn->ring_mmap);
		(void)doca_mmap_destroy(conn->ring_mmap);
		conn->ring_mmap = NULL;
	}
}

/**
 * Wait for the DPU's EXPORT_RCV_RING (sent once the connection is RUNNING)
 *
 * @conn [in]: Connection
 * @mode [in]: Flow mode (for the log line)
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t pull_wait_export(struct wire_conn *conn, uint32_t mode)
{
	struct objects *objs = conn->objs;
	struct timespec start, now;
	long elapsed_ms;

	clock_gettime(CLOCK_MONOTONIC, &start);
	while (!objs->reverse_ready) {
		(void)doca_pe_progress(objs->pe);
		if (objs->peer_gone)
			return DOCA_ERROR_CONNECTION_ABORTED;
		clock_gettime(CLOCK_MONOTONIC, &now);
		elapsed_ms = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
		if (elapsed_ms > WIRE_REV_READY_MS) {
			DOCA_LOG_ERR("No EXPORT_RCV_RING from the DPU within %d ms (does the DPU serve mode %u as an export flow?)",
				     WIRE_REV_READY_MS, mode);
			return DOCA_ERROR_TIME_OUT;
		}
	}
	return DOCA_SUCCESS;
}

/**
 * Import the DPU's exports for this connection
 *
 * The descriptor ring is polled through a DPA memory window
 * (doca_dpa_dev_buf_get_external_ptr), which the SDK supports only for a
 * buffer created on a PF device with the base (non-extended) DPA context; so
 * the ring import lives on the PF even when everything else is on the SF.
 *
 * @conn [in]: Connection
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t pull_import_exports(struct wire_conn *conn)
{
	struct objects *objs = conn->objs;
	struct wire_dev *dev = conn->dev;
	doca_error_t result;

	result = doca_mmap_create_from_export(NULL, objs->rev_msg.tx_desc, objs->rev_msg.tx_desc_len,
					      dev->rev->dev, &conn->tx_mmap);
	if (result == DOCA_SUCCESS)
		result = doca_mmap_start(conn->tx_mmap);
	if (result == DOCA_SUCCESS)
		result = doca_mmap_create_from_export(NULL, objs->rev_msg.ring_desc, objs->rev_msg.ring_desc_len,
						      dev->base_dev, &conn->ring_mmap);
	if (result != DOCA_SUCCESS)
		DOCA_LOG_ERR("Failed to import the DPU exports with error = %s", doca_error_get_name(result));
	return result;
}

/**
 * Create the reverse connection: its PE, DPA thread, msgqs and ring buf_arr
 *
 * @conn [in]: Connection
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t pull_create_thread(struct wire_conn *conn)
{
	struct wire_dev *dev = conn->dev;
	struct dmesh_conn *rc;
	doca_error_t result;

	/* rc's objects: the shared DPA device with this connection's own PE */
	conn->ro = calloc(1, sizeof(*conn->ro));
	if (conn->ro == NULL)
		return DOCA_ERROR_NO_MEMORY;
	conn->ro->dev = dev->rev->dev;
	conn->ro->dpa_pool = dev->rev->dpa_pool;
	conn->ro->dpa_comch = dev->rev->dpa_comch;
	result = doca_pe_create(&conn->ro->consumer_pe);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create PE with error = %s", doca_error_get_name(result));
		return result;
	}

	rc = calloc(1, sizeof(*rc));
	if (rc == NULL)
		return DOCA_ERROR_NO_MEMORY;
	conn->rc = rc;
	rc->objs = conn->ro;
	rc->dpa_thread = calloc(1, sizeof(*rc->dpa_thread));
	if (rc->dpa_thread == NULL)
		return DOCA_ERROR_NO_MEMORY;
	rc->dpa_thread->dpa = dev->rev->dpa_pool->dpa;

	result = dmesh_doca_dpa_thread_create(rc->dpa_thread);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create DPA thread with error = %s", doca_error_get_name(result));
		return result;
	}
	result = init_comch_dpa_msgq(rc, conn->ro->consumer_pe);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to init DPA msgqs with error = %s", doca_error_get_name(result));
		return result;
	}

	/* descriptor ring: buf_array over the imported DPU rcv_ring, on the base context */
	result = doca_buf_arr_create(DMA_RING_SIZE + 1, &rc->buf_arr);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_set_target_dpa(rc->buf_arr, dev->base_dpa);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_set_params(rc->buf_arr, conn->ring_mmap, sizeof(struct dma_desc), 0);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_start(rc->buf_arr);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set up the descriptor ring buf array with error = %s", doca_error_get_name(result));
		return result;
	}

	/* completed-segment ring the recv callback fills; drained by wire_conn_rx_next */
	rc->recv_segs = calloc(DMESH_RECV_SEG_MAX, sizeof(struct dmesh_recv_seg));
	if (rc->recv_segs == NULL)
		return DOCA_ERROR_NO_MEMORY;

	return DOCA_SUCCESS;
}

/**
 * Point the kernel at this connection's window and start it
 *
 * @conn [in]: Connection
 * @cfg [in]: Connection configuration (RX window)
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t pull_run_thread(struct wire_conn *conn, const struct wire_conn_config *cfg)
{
	struct wire_dev *dev = conn->dev;
	struct dmesh_conn *rc = conn->rc;
	struct dpa_thread_arg arg;
	doca_dpa_dev_comch_consumer_completion_t consumer_comp;
	doca_dpa_dev_completion_t producer_comp;
	doca_dpa_dev_comch_producer_t producer;
	doca_dpa_dev_comch_consumer_t consumer;
	doca_dpa_dev_mmap_t src_mmap;
	doca_dpa_dev_buf_arr_t buf_arr;
	doca_dpa_dev_t dpa_dev = 0;
	struct comch_msg kick = {0};
	uint64_t rpc_ret;
	uint8_t *dst;
	doca_error_t result;

	/* DPA handles */
	result = doca_comch_consumer_completion_get_dpa_handle(rc->dpa_comch->consumer_comp, &consumer_comp);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_completion_get_dpa_handle(rc->dpa_comch->producer_comp, &producer_comp);
	if (result == DOCA_SUCCESS)
		result = doca_comch_consumer_get_dpa_handle(rc->dpa_comch->send.consumer, &consumer);
	if (result == DOCA_SUCCESS)
		result = doca_comch_producer_get_dpa_handle(rc->dpa_comch->recv.producer, &producer);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_get_dpa_handle(rc->buf_arr, &buf_arr);
	if (result == DOCA_SUCCESS)
		result = doca_mmap_dev_get_dpa_handle(conn->tx_mmap, dev->rev->dev, &src_mmap);
	if (result == DOCA_SUCCESS && dev->rev->dpa_pool->dpa != dev->base_dpa)
		result = doca_dpa_get_dpa_handle(dev->rev->dpa_pool->dpa, &dpa_dev); /* extended: the kernel switches device */
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA handles with error = %s", doca_error_get_name(result));
		return result;
	}

	/* destination: this connection's window data area, same layout as push */
	dst = (uint8_t *)cfg->rx->buf + cfg->rx_offset + WIRE_PUSH_DATA_OFF;
	arg = (struct dpa_thread_arg) {
		.dpa_consumer_comp = consumer_comp,
		.dpa_producer_comp = producer_comp,
		.dpa_consumer = consumer,
		.dpa_producer = producer,
		.dpa_buf_arr = buf_arr,
		.buf_arr_size = DMA_RING_SIZE,
		.host_mmap = src_mmap,               /* DMA source: DPU tx_staging */
		.dpu_mmap = cfg->rx->dpa_rev,        /* DMA destination: host RX region */
		.src_addr = (uint64_t)(uintptr_t)dst, /* destination base */
		.buf_size = (uint32_t)conn->data_size,
		.rd_pos = 0,
		.rd_fc = 1,                          /* the application's releases gate reuse */
		.dpa_dev = (uint64_t)dpa_dev,
	};

	result = doca_dpa_rpc(rc->dpa_thread->dpa, thread_init_rpc, &rpc_ret, arg.dpa_consumer,
			      (uint32_t)CC_DPA_MAX_MSG_NUM, arg.dpa_dev);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_h2d_memcpy(rc->dpa_thread->dpa, rc->dpa_thread->arg, &arg, sizeof(arg));
	if (result == DOCA_SUCCESS)
		result = doca_dpa_thread_run(rc->dpa_thread->thread);
	/* kick the thread so it enters its poll loop (a thread only wakes on a completion) */
	if (result == DOCA_SUCCESS)
		result = dmesh_doca_dpa_msgq_send(&rc->dpa_comch->send, &kick, sizeof(kick));
	if (result != DOCA_SUCCESS)
		DOCA_LOG_ERR("Failed to start the DPA thread with error = %s", doca_error_get_name(result));
	return result;
}

/**
 * Set up the reverse path of one connection on the pull wire
 *
 * Mirror of the DPU's per-connection setup (and of the legacy host worker's
 * setup_reverse_dpa): import the DPU's exports, bind a DPA thread + msgq to
 * them, point the kernel at this connection's window and kick it.
 *
 * @conn [in]: Connection
 * @cfg [in]: Connection configuration
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t pull_setup(struct wire_conn *conn, const struct wire_conn_config *cfg)
{
	doca_error_t result;

	result = pull_wait_export(conn, cfg->mode);
	if (result != DOCA_SUCCESS)
		return result;
	result = pull_import_exports(conn);
	if (result != DOCA_SUCCESS)
		return result;
	result = pull_create_thread(conn);
	if (result != DOCA_SUCCESS)
		return result;
	return pull_run_thread(conn, cfg);
}

/*
 * ---------------------------------------------------------------------------
 * Connections
 * ---------------------------------------------------------------------------
 */

/**
 * Release a connection
 *
 * Mirrors the DPUMesh host library's close: the reverse DPA thread first, then
 * stop the producer, then the client, progressing their engines until idle,
 * then release the ring. The shared pool and region mmaps belong to the
 * carrier and are left alone.
 *
 * @conn [in]: Connection
 */
static void conn_teardown(struct wire_conn *conn)
{
	struct objects *objs = conn->objs;

	if (conn->dev != NULL && conn->dev->pull)
		pull_teardown(conn);

	if (objs->producer != NULL) {
		(void)doca_ctx_stop(doca_comch_producer_as_ctx(objs->producer));
		wait_ctx_idle(doca_comch_producer_as_ctx(objs->producer),
			      objs->producer_pe != NULL ? objs->producer_pe : objs->pe);
		(void)doca_comch_producer_destroy(objs->producer);
		objs->producer = NULL;
	}
	if (objs->producer_mem != NULL) {
		clean_local_mem_bufs(objs->producer_mem);
		free(objs->producer_mem);
		objs->producer_mem = NULL;
	}
	if (objs->producer_pe != NULL) {
		(void)doca_pe_destroy(objs->producer_pe);
		objs->producer_pe = NULL;
	}
	if (objs->cc_client != NULL) {
		(void)doca_ctx_stop(doca_comch_client_as_ctx(objs->cc_client));
		wait_ctx_idle(doca_comch_client_as_ctx(objs->cc_client), objs->pe);
		(void)doca_comch_client_destroy(objs->cc_client);
		objs->cc_client = NULL;
		objs->cc_server = NULL;
	}
	if (objs->dma_ring != NULL) {
		if (objs->dma_ring->mmap != NULL)
			(void)destroy_mmap_and_free_buffer(objs->dma_ring->mmap, objs->dma_ring->buffer);
		free(objs->dma_ring);
		objs->dma_ring = NULL;
	}

	/* shared, owned by the carrier */
	objs->sndbuf.mmap = NULL;
	objs->rcvbuf.mmap = NULL;
	objs->dev = NULL;

	cleanup_objects(objs);
	free(objs);
	free(conn);
}

int wire_conn_open(struct wire_dev *dev, const struct wire_conn_config *cfg, struct wire_conn **out)
{
	struct wire_conn *conn;
	struct objects *objs;
	doca_error_t result;

	conn = calloc(1, sizeof(*conn));
	objs = conn != NULL ? calloc(1, sizeof(*objs)) : NULL;
	if (conn == NULL || objs == NULL) {
		free(conn);
		free(objs);
		errno = ENOMEM;
		return -1;
	}
	conn->objs = objs;
	conn->dev = dev;
	objs->dev = dev->dev;

	/* flow identity */
	objs->flow.src_ip = cfg->src_ip;
	objs->flow.dst_ip = cfg->dst_ip;
	objs->flow.src_port = cfg->src_port;
	objs->flow.dst_port = cfg->dst_port;
	objs->flow.mode = cfg->mode;
	snprintf(objs->flow.src_workload, sizeof(objs->flow.src_workload), "%s",
		 cfg->workload != NULL ? cfg->workload : "");

	/* Comch control path + the producer the DPU consumer expects */
	result = init_comch_ctrl_path_client(cfg->server, objs, true);
	if (result != DOCA_SUCCESS)
		goto fail;
	result = init_comch_datapath_producer(objs);
	if (result != DOCA_SUCCESS)
		goto fail;

	/* forward ring and the shared regions as this connection's sndbuf/rcvbuf */
	if (setup_dma_ring(objs, WIRE_RING_SIZE) != 0) {
		result = DOCA_ERROR_NO_MEMORY;
		goto fail;
	}
	objs->sndbuf.mmap = cfg->tx->mmap;
	objs->sndbuf.buf = cfg->tx->buf;
	objs->sndbuf.size = cfg->tx->bytes;
	objs->rcvbuf.mmap = cfg->rx->mmap;
	objs->rcvbuf.buf = (char *)cfg->rx->buf + cfg->rx_offset;
	objs->rcvbuf.size = WIRE_PUSH_WINDOW;

	/* push layout of the window: slot ring, cursor, data ring */
	memset(objs->rcvbuf.buf, 0, DMESH_PUSH_DATA_OFF);
	conn->descs = (volatile struct dmesh_push_desc *)objs->rcvbuf.buf;
	conn->cursor = (volatile struct dmesh_push_cursor *)((char *)objs->rcvbuf.buf + DMESH_PUSH_CURSOR_OFF);
	conn->cursor->consumed_seq = 0;
	conn->cursor->consumed_bytes = 0;
	conn->cursor->magic = DMESH_PUSH_FC_MAGIC;
	conn->data_size = WIRE_PUSH_WINDOW - DMESH_PUSH_DATA_OFF;
	conn->expected = 1;
	conn->tx_dpa = cfg->tx->dpa;

	/* hand the rings to the DPU */
	result = export_dma_metadata(objs);
	if (result != DOCA_SUCCESS)
		goto fail;

	if (dev->pull) {
		result = pull_setup(conn, cfg);
		if (result != DOCA_SUCCESS)
			goto fail;
	}

	*out = conn;
	return 0;

fail:
	conn_teardown(conn);
	errno = error_number(result);
	return -1;
}

void wire_conn_close(struct wire_conn *conn)
{
	if (conn == NULL || conn->closed)
		return;
	conn->closed = 1;
	conn_teardown(conn);
}

int wire_conn_progress(struct wire_conn *conn)
{
	(void)doca_pe_progress(conn->objs->pe);
	if (conn->objs->producer_pe != NULL)
		(void)doca_pe_progress(conn->objs->producer_pe);
	if (conn->rc != NULL)
		(void)doca_pe_progress(conn->ro->consumer_pe); /* reverse DMA completions */
	return conn->objs->peer_gone;
}

/*
 * ---------------------------------------------------------------------------
 * Doorbells
 * ---------------------------------------------------------------------------
 */

/**
 * Collect the progress engines of a connection
 *
 * @conn [in]: Connection
 * @pes [out]: Engines (WIRE_CONN_FDS at most)
 * @return: Number of engines
 */
static int conn_engines(struct wire_conn *conn, struct doca_pe **pes)
{
	int n = 0;

	pes[n++] = conn->objs->pe;
	if (conn->objs->producer_pe != NULL)
		pes[n++] = conn->objs->producer_pe;
	if (conn->rc != NULL)
		pes[n++] = conn->ro->consumer_pe;
	return n;
}

int wire_conn_fds(struct wire_conn *conn, int *fds, int max)
{
	struct doca_pe *pes[WIRE_CONN_FDS];
	doca_notification_handle_t handle;
	int n = conn_engines(conn, pes);
	int i, count = 0;

	for (i = 0; i < n && count < max; i++) {
		if (doca_pe_get_notification_handle(pes[i], &handle) != DOCA_SUCCESS)
			continue;
		fds[count++] = (int)handle;
	}
	return count;
}

int wire_conn_arm(struct wire_conn *conn)
{
	struct doca_pe *pes[WIRE_CONN_FDS];
	int n = conn_engines(conn, pes);
	int i;
	doca_error_t result;

	for (i = 0; i < n; i++) {
		result = doca_pe_request_notification(pes[i]);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to arm progress engine with error = %s", doca_error_get_name(result));
			errno = error_number(result);
			return -1;
		}
	}
	return 0;
}

void wire_conn_clear(struct wire_conn *conn, int fd)
{
	struct doca_pe *pes[WIRE_CONN_FDS];
	doca_notification_handle_t handle;
	int n = conn_engines(conn, pes);
	int i;

	for (i = 0; i < n; i++) {
		if (doca_pe_get_notification_handle(pes[i], &handle) != DOCA_SUCCESS || (int)handle != fd)
			continue;
		(void)doca_pe_clear_notification(pes[i], handle);
		return;
	}
}

/*
 * ---------------------------------------------------------------------------
 * Forward path
 * ---------------------------------------------------------------------------
 */

uint32_t wire_conn_ring_free(const struct wire_conn *conn)
{
	struct dma_ring *ring = conn->objs->dma_ring;
	uint64_t used = ring->head - ring->ctrl->consumer_head;

	return used >= ring->size ? 0 : (uint32_t)(ring->size - used);
}

uint64_t wire_conn_post(struct wire_conn *conn, uint64_t addr, uint32_t bytes)
{
	struct dma_ring *ring = conn->objs->dma_ring;
	struct dma_desc *desc = get_next_dma_desc(ring); /* the caller checked ring_free */

	desc->mmap = conn->tx_dpa;
	desc->addr = addr;
	desc->size = bytes;
	commit_dma_desc(ring);
	return ring->head;
}

uint64_t wire_conn_consumed(const struct wire_conn *conn)
{
	return conn->objs->dma_ring->ctrl->consumer_head;
}

/*
 * ---------------------------------------------------------------------------
 * Reverse path
 * ---------------------------------------------------------------------------
 */

/**
 * Pull wire: next completed DPA copy, delivered in order
 *
 * The carrier tracks at most WIRE_PUSH_DESC_N live batches, so a segment is
 * handed out only while that many are outstanding.
 *
 * @conn [in]: Connection
 * @seq [out]: Batch sequence
 * @pos [out]: Offset in the window's data ring
 * @len [out]: Length
 * @return: 1 with a batch, 0 when none, -1 on a malformed segment
 */
static int pull_rx_next(struct wire_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
	struct dmesh_conn *rc = conn->rc;
	struct dmesh_recv_seg *seg;
	uint32_t p, n;

	if (rc->recv_seg_cnt == 0 || conn->rx_seq - conn->consumed_seq >= WIRE_PUSH_DESC_N)
		return 0;

	seg = &rc->recv_segs[rc->recv_seg_head];
	p = seg->pos;
	n = seg->len;
	rc->recv_seg_head = (rc->recv_seg_head + 1) % DMESH_RECV_SEG_MAX;
	rc->recv_seg_cnt--;
	if (n == 0 || (size_t)p + n > conn->data_size)
		return -1;

	conn->rx_seq++;
	conn->seg_end[conn->rx_seq % WIRE_PUSH_DESC_N] = p + n;
	*seq = conn->rx_seq;
	*pos = p;
	*len = n;
	return 1;
}

int wire_conn_rx_next(struct wire_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
	volatile struct dmesh_push_desc *desc;
	uint32_t p, n;

	if (conn->rc != NULL)
		return pull_rx_next(conn, seq, pos, len);

	/* push: the next slot the DPU's DMA engine filled */
	desc = &conn->descs[conn->expected % DMESH_PUSH_DESC_N];
	if (desc->seq != conn->expected)
		return 0;
	p = desc->pos;
	n = desc->len;
	if (n == 0 || (size_t)p + n > conn->data_size)
		return -1;

	*seq = conn->expected;
	*pos = p;
	*len = n;
	conn->expected++;
	return 1;
}

/**
 * Pull wire: publish the kernel's read watermark
 *
 * The watermark is the end of the newest released segment (copies land in
 * order; the kernel wraps a copy that would cross the end, so bytes do not map
 * linearly to offsets). The device-side write is coalesced: the kernel gates
 * only when fewer than 3 x 8064 B of the 1 MiB ring look free, so publishing
 * every WIRE_RD_POS_BATCH bytes keeps it far from the gate.
 *
 * @conn [in]: Connection
 * @seq [in]: Newest released batch
 * @bytes [in]: Bytes released so far
 */
static void pull_rx_consumed(struct wire_conn *conn, uint64_t seq, uint64_t bytes)
{
	if (seq <= conn->consumed_seq)
		return;
	conn->consumed_seq = seq;
	conn->rd_pos = conn->seg_end[seq % WIRE_PUSH_DESC_N] % (uint32_t)conn->data_size;

	if (bytes - conn->rd_published_bytes < WIRE_RD_POS_BATCH &&
	    seq - conn->rd_published_seq < WIRE_PUSH_DESC_N / 2)
		return;
	conn->rd_published_bytes = bytes;
	conn->rd_published_seq = seq;
	(void)doca_dpa_h2d_memcpy(conn->rc->dpa_thread->dpa,
				  conn->rc->dpa_thread->arg + offsetof(struct dpa_thread_arg, rd_pos),
				  &conn->rd_pos, sizeof(conn->rd_pos));
}

void wire_conn_rx_consumed(struct wire_conn *conn, uint64_t seq, uint64_t bytes)
{
	if (conn->rc != NULL) {
		pull_rx_consumed(conn, seq, bytes);
		return;
	}
	/* push: the DPU pulls this cursor for its flow control */
	conn->cursor->consumed_seq = seq;
	conn->cursor->consumed_bytes = bytes;
}
