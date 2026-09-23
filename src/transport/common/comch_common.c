#include "comch_common.h"

#include <doca_log.h>
#include <doca_error.h>
#include <doca_mmap.h>
#include <doca_dpa.h>

#include "object.h"
#include "comch_client.h"
#include "comch_server.h"
#include "dpa_common.h"
#include "dpa.h"
#include "ring.h"
#include "buffer.h"
#include "dma.h"          /* BUFFER_SIZE */
DOCA_LOG_REGISTER(COMCH_COMMON);

/*
 * Export all host-side DMA metadata (ring + send buffer + receive buffer) to
 * the DPU in a single control-path message. Must be called after
 * setup_dma_ring() and both init_dmesh_buffer() calls.
 */
doca_error_t
export_dma_metadata(struct objects *objs)
{
    struct dmesh_buffer *sndbuf, *rcvbuf;
    struct dmesh_export_metadata_msg *msg;
    doca_error_t result;
    const void *export_desc;
    size_t export_desc_len;

    sndbuf = &objs->sndbuf;
    rcvbuf = &objs->rcvbuf;

    msg = (struct dmesh_export_metadata_msg *)malloc(sizeof(struct dmesh_export_metadata_msg));
    if (msg == NULL) {
        DOCA_LOG_ERR("Failed to allocate memory for metadata export message");
        return DOCA_ERROR_NO_MEMORY;
    }

    msg->type = DMESH_MSG_EXPORT_METADATA;
    msg->flow = objs->flow;

    /* DMA ring */
    result = doca_mmap_export_pci(objs->dma_ring->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export DMA ring mmap to DPU: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->ring_buf = objs->dma_ring->buffer;
    msg->ring_buf_len = sizeof(struct dma_ring_ctrl) + objs->dma_ring->size * sizeof(struct dma_desc);
    msg->ring_desc_len = export_desc_len;
    memcpy(msg->ring_desc, export_desc, export_desc_len);
    DOCA_LOG_INFO("export mmap of DMA ring: descriptor length: %zu bytes", export_desc_len);

    /* send buffer */
    result = doca_mmap_export_pci(sndbuf->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export send buffer mmap to DPU: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->sndbuf = sndbuf->buf;
    msg->sndbuf_len = sndbuf->size;
    msg->snd_desc_len = export_desc_len;
    memcpy(msg->snd_desc, export_desc, export_desc_len);
    DOCA_LOG_INFO("export mmap of sndbuf: descriptor length: %zu bytes", export_desc_len);

    /* receive buffer */
    result = doca_mmap_export_pci(rcvbuf->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export receive buffer mmap to DPU: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->rcvbuf = rcvbuf->buf;
    msg->rcvbuf_len = rcvbuf->size;
    msg->rcv_desc_len = export_desc_len;
    memcpy(msg->rcv_desc, export_desc, export_desc_len);
    DOCA_LOG_INFO("export mmap of rcvbuf: descriptor length: %zu bytes", export_desc_len);

    result = client_send_msg(objs, (const char *)msg, sizeof(struct dmesh_export_metadata_msg));
    free(msg);
    return result;
}

/*
 * DPU-side handler for the combined metadata message: creates the ring mmap
 * and both remote buffer mmaps in one shot.
 */
doca_error_t
process_export_metadata_msg(struct dmesh_conn *conn, struct dmesh_export_metadata_msg *metadata_msg)
{
    struct objects *objs = conn->objs;
    doca_error_t result;

    conn->flow = metadata_msg->flow;
    conn->flow.src_workload[sizeof(conn->flow.src_workload) - 1] = '\0';

    DOCA_LOG_INFO("Received export metadata message: flow %u.%u.%u.%u:%u -> %u.%u.%u.%u:%u (%s), "
                  "ring_buf=%p, ring_buf_len=%zu, sndbuf_len=%zu, rcvbuf_len=%zu",
                  conn->flow.src_ip & 0xff, (conn->flow.src_ip >> 8) & 0xff,
                  (conn->flow.src_ip >> 16) & 0xff, (conn->flow.src_ip >> 24) & 0xff,
                  conn->flow.src_port,
                  conn->flow.dst_ip & 0xff, (conn->flow.dst_ip >> 8) & 0xff,
                  (conn->flow.dst_ip >> 16) & 0xff, (conn->flow.dst_ip >> 24) & 0xff,
                  conn->flow.dst_port,
                  conn->flow.src_workload,
                  metadata_msg->ring_buf, metadata_msg->ring_buf_len,
                  metadata_msg->sndbuf_len, metadata_msg->rcvbuf_len);

    /* DMA ring */
    result = doca_mmap_create_from_export(NULL, metadata_msg->ring_desc,
                                            metadata_msg->ring_desc_len,
                                            objs->dev, &conn->ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for DMA ring from export desc with error = %s",
                     doca_error_get_name(result));
        return result;
    }

    /* send buffer */
    result = doca_mmap_create_from_export(NULL, metadata_msg->snd_desc,
                                            metadata_msg->snd_desc_len,
                                            objs->dev, &conn->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for send buffer from export desc with error = %s",
                     doca_error_get_name(result));
        goto destroy_ring_mmap;
    }
    conn->sndbuf.buf = metadata_msg->sndbuf;
    conn->sndbuf.size = metadata_msg->sndbuf_len;

    /* receive buffer */
    result = doca_mmap_create_from_export(NULL, metadata_msg->rcv_desc,
                                            metadata_msg->rcv_desc_len,
                                            objs->dev, &conn->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for receive buffer from export desc with error = %s",
                     doca_error_get_name(result));
        goto destroy_sndbuf_mmap;
    }
    conn->rcvbuf.buf = metadata_msg->rcvbuf;
    conn->rcvbuf.size = metadata_msg->rcvbuf_len;

    result = doca_mmap_start(conn->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start send buffer mmap with error = %s", doca_error_get_name(result));
        goto destroy_rcvbuf_mmap;
    }

    result = doca_mmap_start(conn->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start receive buffer mmap with error = %s", doca_error_get_name(result));
        goto destroy_rcvbuf_mmap;
    }

    DOCA_LOG_INFO("Successfully created remote mmaps for DMA ring, send and receive buffers");

    return DOCA_SUCCESS;

destroy_rcvbuf_mmap:
    doca_mmap_destroy(conn->rcvbuf.mmap);
    conn->rcvbuf.mmap = NULL;
destroy_sndbuf_mmap:
    doca_mmap_destroy(conn->sndbuf.mmap);
    conn->sndbuf.mmap = NULL;
destroy_ring_mmap:
    doca_mmap_destroy(conn->ring_mmap);
    conn->ring_mmap = NULL;
    return result;
}

doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg)
{
    objs->remote_dpa_producer = dpa_comp_msg->dpa_producer;
    objs->remote_dpa_producer_comp = dpa_comp_msg->dpa_producer_comp;

    return DOCA_SUCCESS;
}

/* DPU side: allocate this connection's reverse rcv_ring + tx_staging (once) and
 * ship their PCI export descriptors to the host. The mirror of
 * export_dma_metadata, but DPU -> host (server -> client). */
doca_error_t
export_rcv_ring_metadata(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    struct dmesh_export_rcv_ring_msg *msg;
    doca_error_t result;
    const void *export_desc;
    size_t export_desc_len;

    if (conn->reverse_exported)
        return DOCA_SUCCESS;

    if (conn->rcv_ring == NULL) {
        result = alloc_dma_ring(&conn->rcv_ring, objs->dev, DMA_RING_SIZE);
        if (result != DOCA_SUCCESS)
            return result;
    }
    if (conn->tx_staging == NULL) {
        result = alloc_buffer_and_set_mmap(&conn->tx_staging_mmap, objs->dev,
                                           &conn->tx_staging, BUFFER_SIZE,
                                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
        if (result != DOCA_SUCCESS)
            return result;
        conn->tx_staging_len = BUFFER_SIZE;
        conn->tx_pos = 0;
    }

    msg = (struct dmesh_export_rcv_ring_msg *)malloc(sizeof(*msg));
    if (msg == NULL)
        return DOCA_ERROR_NO_MEMORY;
    memset(msg, 0, sizeof(*msg));
    msg->type = DMESH_MSG_EXPORT_RCV_RING;

    /* descriptor ring */
    result = doca_mmap_export_pci(conn->rcv_ring->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export reverse rcv_ring mmap: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->ring_buf = conn->rcv_ring->buffer;
    msg->ring_buf_len = sizeof(struct dma_ring_ctrl) + conn->rcv_ring->size * sizeof(struct dma_desc);
    msg->ring_desc_len = export_desc_len;
    memcpy(msg->ring_desc, export_desc, export_desc_len);

    /* tx staging buffer */
    result = doca_mmap_export_pci(conn->tx_staging_mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export reverse tx_staging mmap: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->tx_staging = conn->tx_staging;
    msg->tx_staging_len = conn->tx_staging_len;
    msg->tx_desc_len = export_desc_len;
    memcpy(msg->tx_desc, export_desc, export_desc_len);

    result = server_send_msg_conn(objs, conn->connection, (const char *)msg, sizeof(*msg));
    if (result != DOCA_SUCCESS) {
        free(msg);
        return result;
    }

    conn->reverse_exported = true;
    DOCA_LOG_INFO("Exported reverse rcv_ring (buf=%p len=%zu) + tx_staging (buf=%p len=%zu) to host",
                  msg->ring_buf, msg->ring_buf_len, msg->tx_staging, msg->tx_staging_len);
    free(msg);
    return DOCA_SUCCESS;
}

/* Host side: stash the DPU's export descriptors. The actual mmap import + the
 * reverse DPA thread are set up by the host worker (setup_reverse_dpa) once it
 * has opened the reverse PCI function (94:00.0) - a flexio process is
 * one-per-function and the DPU proxy already holds this host's 94:00.1, so the
 * reverse DPA must live on a different function and import on that device. */
doca_error_t
process_export_rcv_ring_msg(struct objects *objs, struct dmesh_export_rcv_ring_msg *msg)
{
    objs->rev_msg = *msg;
    objs->reverse_ready = true;
    DOCA_LOG_INFO("Stashed reverse rcv_ring (%p, %zu) + tx_staging (%p, %zu) export from DPU",
                  msg->ring_buf, msg->ring_buf_len, msg->tx_staging, msg->tx_staging_len);
    return DOCA_SUCCESS;
}
