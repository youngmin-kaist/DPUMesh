#include "ring.h"
#include <stdlib.h>
#include <string.h>
#include <doca_log.h>
#include "dpa_common.h"
#include "object.h"
#include "buffer.h"
#include "comch_common.h"

DOCA_LOG_REGISTER(RING);

#define DMA_RING_LOG_INTERVAL 1024

int setup_dma_ring(struct objects *objs, size_t size)
{
    doca_error_t result;
    struct dma_ring *ring;

    if (objs->dma_ring == NULL) {
        objs->dma_ring = (struct dma_ring *)malloc(sizeof(struct dma_ring));
    }

    ring = objs->dma_ring;
    ring->size = size;
    ring->head = 0;
    ring->tail = 0;
    ring->buffer = NULL;
    ring->ctrl = NULL;
    ring->descs = NULL;

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_buffer_and_set_mmap(&ring->mmap, objs->dev,
                           &ring->buffer,
                           sizeof(struct dma_ring_ctrl) + ring->size * sizeof(struct dma_desc),
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        return result;
    }

    memset(ring->buffer, 0, sizeof(struct dma_ring_ctrl) + ring->size * sizeof(struct dma_desc));
    ring->ctrl = (struct dma_ring_ctrl *)ring->buffer;
    ring->descs = (struct dma_desc *)((uint8_t *)ring->buffer + sizeof(struct dma_ring_ctrl));
    
    /* export mmap to DPU */
    result = export_dma_ring(objs);
    // result = export_mmap_to_remote(objs, ring->mmap,
    //                                ring->buffer,
    //                                sizeof(struct dma_ring_ctrl) + ring->size * sizeof(struct dma_desc),
    //                                DMA_RING, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        destroy_mmap_and_free_buffer(ring->mmap, ring->buffer);
        return result;
    }
    return 0;
}

struct dma_desc *get_next_dma_desc(struct dma_ring *ring)
{
    uint64_t ring_mask = ring->size - 1;

    while (ring->head - ring->ctrl->consumer_head >= ring->size) {
        // DOCA_LOG_INFO("Host ring full: producer_tail=%lu, local_head=%lu, consumer_head=%lu",
        //               ring->ctrl->producer_tail, ring->head, ring->ctrl->consumer_head);
    }

    struct dma_desc *desc = ring->descs + (ring->head & ring_mask);
    ring->head++;
    // DOCA_LOG_INFO("Get next DMA desc - head: %u, tail: %u, desc: %p", ring->head, ring->tail, desc);
    return desc;
}

void commit_dma_desc(struct dma_ring *ring)
{
    __sync_synchronize();
    ring->ctrl->producer_tail = ring->head;

    if ((ring->head & (DMA_RING_LOG_INTERVAL - 1)) == 0) {
        DOCA_LOG_INFO("Host ring publish: producer_tail=%lu, consumer_head=%lu",
                      ring->ctrl->producer_tail, ring->ctrl->consumer_head);
    }
}
