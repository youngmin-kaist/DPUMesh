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
process_export_metadata_msg(struct objects* objs, struct dmesh_export_metadata_msg *metadata_msg)
{
    doca_error_t result;

    DOCA_LOG_INFO("Received export metadata message: ring_buf=%p, ring_buf_len=%zu, "
                  "sndbuf=%p, sndbuf_len=%zu, rcvbuf=%p, rcvbuf_len=%zu",
                  metadata_msg->ring_buf, metadata_msg->ring_buf_len,
                  metadata_msg->sndbuf, metadata_msg->sndbuf_len,
                  metadata_msg->rcvbuf, metadata_msg->rcvbuf_len);

    /* DMA ring */
    result = doca_mmap_create_from_export(NULL, metadata_msg->ring_desc,
                                            metadata_msg->ring_desc_len,
                                            objs->dev, &objs->ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for DMA ring from export desc with error = %s",
                     doca_error_get_name(result));
        return result;
    }

    /* send buffer */
    result = doca_mmap_create_from_export(NULL, metadata_msg->snd_desc,
                                            metadata_msg->snd_desc_len,
                                            objs->dev, &objs->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for send buffer from export desc with error = %s",
                     doca_error_get_name(result));
        goto destroy_ring_mmap;
    }
    objs->sndbuf.buf = metadata_msg->sndbuf;
    objs->sndbuf.size = metadata_msg->sndbuf_len;

    /* receive buffer */
    result = doca_mmap_create_from_export(NULL, metadata_msg->rcv_desc,
                                            metadata_msg->rcv_desc_len,
                                            objs->dev, &objs->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for receive buffer from export desc with error = %s",
                     doca_error_get_name(result));
        goto destroy_sndbuf_mmap;
    }
    objs->rcvbuf.buf = metadata_msg->rcvbuf;
    objs->rcvbuf.size = metadata_msg->rcvbuf_len;

    result = doca_mmap_start(objs->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start send buffer mmap with error = %s", doca_error_get_name(result));
        goto destroy_rcvbuf_mmap;
    }

    result = doca_mmap_start(objs->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start receive buffer mmap with error = %s", doca_error_get_name(result));
        goto destroy_rcvbuf_mmap;
    }

    DOCA_LOG_INFO("Successfully created remote mmaps for DMA ring, send and receive buffers");

    return DOCA_SUCCESS;

destroy_rcvbuf_mmap:
    doca_mmap_destroy(objs->rcvbuf.mmap);
    objs->rcvbuf.mmap = NULL;
destroy_sndbuf_mmap:
    doca_mmap_destroy(objs->sndbuf.mmap);
    objs->sndbuf.mmap = NULL;
destroy_ring_mmap:
    doca_mmap_destroy(objs->ring_mmap);
    objs->ring_mmap = NULL;
    return result;
}

doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg)
{
    objs->remote_dpa_producer = dpa_comp_msg->dpa_producer;
    objs->remote_dpa_producer_comp = dpa_comp_msg->dpa_producer_comp;

    return DOCA_SUCCESS;
}
