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

doca_error_t
export_dma_buffer(struct objects *objs)
{
    struct dmesh_buffer *sndbuf, *rcvbuf;
    struct dmesh_export_buf_msg *msg;
    doca_error_t result;
    const void *export_desc;
    size_t export_desc_len;

    sndbuf = &objs->sndbuf;
    rcvbuf = &objs->rcvbuf;

    msg = (struct dmesh_export_buf_msg *)malloc(sizeof(struct dmesh_export_buf_msg));
    if (msg == NULL) {
        DOCA_LOG_ERR("Failed to allocate memory for mmap export message");
        return DOCA_ERROR_NO_MEMORY;
    }

    msg->type = DMESH_MSG_EXPORT_BUFFER;
    msg->mmap_type = DMA_BUFFER;
    msg->sndbuf = sndbuf->buf;
    msg->rcvbuf = rcvbuf->buf;
    msg->sndbuf_len = sndbuf->size;
    msg->rcvbuf_len = rcvbuf->size;

    /* export send buffer mmap to DPU */
    result = doca_mmap_export_pci(sndbuf->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export send buffer mmap to DPU: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }

    DOCA_LOG_INFO("export mmap of sndbuf:  descriptor length: %zu bytes",
                  export_desc_len);
    memcpy(msg->snd_desc, export_desc, export_desc_len);
    msg->snd_desc_len = export_desc_len;
        
    result = doca_mmap_export_pci(rcvbuf->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export receive buffer mmap to DPU: %s",
                doca_error_get_descr(result));
        free(msg);
        return result;
    }

    DOCA_LOG_INFO("export mmap of rcvbuf:  descriptor length: %zu bytes",
                  export_desc_len);
    memcpy(msg->rcv_desc, export_desc, export_desc_len);
    msg->rcv_desc_len = export_desc_len;

    result = client_send_msg(objs, (const char *)msg, sizeof(struct dmesh_export_buf_msg));
    free(msg);
    return result;
}

doca_error_t
export_dma_ring(struct objects *objs)
{
    doca_error_t result;
    struct dmesh_export_ring_msg *msg;
    const void *export_desc;
    size_t export_desc_len;

    result = doca_mmap_export_pci(objs->dma_ring->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export DMA ring mmap to DPU: %s", doca_error_get_descr(result));
        return result;
    }

    msg = (struct dmesh_export_ring_msg *)malloc(sizeof(struct dmesh_export_ring_msg));
    if (msg == NULL) {
        DOCA_LOG_ERR("Failed to allocate memory for DMA ring export message");
        return DOCA_ERROR_NO_MEMORY;
    }
    msg->type = DMESH_MSG_EXPORT_RING;
    msg->mmap_type = DMA_RING;
    msg->buf = objs->dma_ring->buffer;
    msg->buf_len = sizeof(struct dma_ring_ctrl) + objs->dma_ring->size * sizeof(struct dma_desc);
    msg->desc_len = export_desc_len;
    memcpy(msg->desc, export_desc, export_desc_len);

    result = client_send_msg(objs, (const char *)msg, sizeof(struct dmesh_export_ring_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send DMA ring export message to DPU: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }

    free(msg);
    return DOCA_SUCCESS;
}
// doca_error_t
// export_mmap_to_remote(struct objects *objs, struct doca_mmap *mmap, void *buffer, size_t buf_size, enum mmap_type mmap_type, enum msg_direction direction)
// {
//     doca_error_t result;
//     struct dmesh_mmap_msg *msg;
//     const void *export_desc;
// 	size_t export_desc_len;
//     char export_msg[4096];

//     result = doca_mmap_export_pci(mmap, objs->dev, &export_desc, &export_desc_len);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to export local mmap to DPU: %s", doca_error_get_descr(result));
//         return result;
//     }

//     DOCA_LOG_INFO("Successfully exported local mmap to DPU, export descriptor length: %zu bytes",
//                   export_desc_len);

//     msg = (struct dmesh_mmap_msg *)export_msg;
//     msg->type = DMESH_MSG_EXPORT_DESC;
//     msg->mmap_type = mmap_type;
//     msg->host_addr = (void *)htonq((uint64_t)buffer);
//     msg->buf_size = htonq((uint64_t)buf_size);
//     msg->export_desc_len = htonq(export_desc_len);
//     memcpy(msg->export_desc, export_desc, export_desc_len);
    
//     /* Send export descriptor to DPU via comch */
//     if (direction == HOST_TO_DPU) {
//         return client_send_msg(objs, (const char *)msg, sizeof(struct dmesh_mmap_msg) + export_desc_len);
//     } else {
//         return server_send_msg(objs, (const char *)msg, sizeof(struct dmesh_mmap_msg) + export_desc_len);
//     }
// }

doca_error_t
process_export_ring_msg(struct objects* objs, struct dmesh_export_ring_msg *export_ring_msg)
{
    doca_error_t result;
    DOCA_LOG_INFO("Received export ring message: buf=%p, buf_len=%zu, desc_len=%zu",
                  export_ring_msg->buf, export_ring_msg->buf_len, export_ring_msg->desc_len);

    result = doca_mmap_create_from_export(NULL, export_ring_msg->desc, 
                                            export_ring_msg->desc_len, 
                                            objs->dev, &objs->ring_mmap);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create remote mmap from export desc with error = %s",
			     doca_error_get_name(result));
		return result;
	}   

	return DOCA_SUCCESS;
}

doca_error_t
process_export_buf_msg(struct objects* objs, struct dmesh_export_buf_msg *export_buf_msg)
{    // Implementation for processing export buffer message
    
    doca_error_t result;
    DOCA_LOG_INFO("Received export buffer message: sndbuf=%p, sndbuf_len=%zu, rcvbuf=%p, rcvbuf_len=%zu, snd_desc_len=%zu, rcv_desc_len=%zu",
                  export_buf_msg->sndbuf, export_buf_msg->sndbuf_len,
                  export_buf_msg->rcvbuf, export_buf_msg->rcvbuf_len,
                  export_buf_msg->snd_desc_len, export_buf_msg->rcv_desc_len);
                  

    result = doca_mmap_create_from_export(NULL, export_buf_msg->snd_desc, 
                                            export_buf_msg->snd_desc_len, 
                                            objs->dev, &objs->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for send buffer from export desc with error = %s",
                     doca_error_get_name(result));
        return result;
    }
    objs->sndbuf.buf = export_buf_msg->sndbuf;
    objs->sndbuf.size = export_buf_msg->sndbuf_len;

    result = doca_mmap_create_from_export(NULL, export_buf_msg->rcv_desc, 
                                            export_buf_msg->rcv_desc_len, 
                                            objs->dev, &objs->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for receive buffer from export desc with error = %s",
                     doca_error_get_name(result));
        doca_mmap_destroy(objs->sndbuf.mmap);
        return result;
    }
    objs->rcvbuf.buf = export_buf_msg->rcvbuf;
    objs->rcvbuf.size = export_buf_msg->rcvbuf_len;

    result = doca_mmap_start(objs->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start send buffer mmap with error = %s", doca_error_get_name(result));
        doca_mmap_destroy(objs->sndbuf.mmap);
        doca_mmap_destroy(objs->rcvbuf.mmap);
        return result;
    }

    result = doca_mmap_start(objs->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start receive buffer mmap with error = %s", doca_error_get_name(result));
        doca_mmap_destroy(objs->sndbuf.mmap);
        doca_mmap_destroy(objs->rcvbuf.mmap);
        return result;
    }

    DOCA_LOG_INFO("Successfully created remote mmaps for send and receive buffers");

    return DOCA_SUCCESS;
}

// doca_error_t
// process_mmap_msg(struct objects *objs, struct dmesh_mmap_msg *mmap_msg)
// {
// 	doca_error_t result;
//     struct doca_mmap **mmap;
// 	void *remote_addr = (void *)ntohq((uint64_t)mmap_msg->host_addr);
// 	size_t buf_size = ntohq(mmap_msg->buf_size);
// 	size_t export_desc_len = ntohq(mmap_msg->export_desc_len);

// 	DOCA_LOG_INFO("remote_addr: %p, buf_size: %zu, export_desc_len: %zu",
// 		      remote_addr, buf_size, export_desc_len);

//     if (mmap_msg->mmap_type == DMA_BUFFER) {
//         mmap = &objs->remote_mmap;
//     } else if (mmap_msg->mmap_type == DMA_RING) {
//         mmap = &objs->ring_mmap;
//     } else {
//         DOCA_LOG_ERR("Invalid mmap type received: %d", mmap_msg->mmap_type);
//         return DOCA_ERROR_INVALID_VALUE;
//     }

// 	result = doca_mmap_create_from_export(NULL, mmap_msg->export_desc,
// 					      export_desc_len,
// 					      objs->dev,
// 					      mmap);
// 	if (result != DOCA_SUCCESS) {
// 		DOCA_LOG_ERR("Failed to create remote mmap from export desc with error = %s",
// 			     doca_error_get_name(result));
// 		return result;
// 	}

// 	objs->remote_addr = remote_addr;
// 	objs->remote_buf_size = buf_size;

// 	return DOCA_SUCCESS;
// }

doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg)
{
    objs->remote_dpa_producer = dpa_comp_msg->dpa_producer;
    objs->remote_dpa_producer_comp = dpa_comp_msg->dpa_producer_comp;

    return DOCA_SUCCESS;
}
