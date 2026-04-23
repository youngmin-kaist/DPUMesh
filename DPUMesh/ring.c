#include "ring.h"
#include <stdlib.h>
#include <doca_log.h>
#include "dpa_common.h"
#include "object.h"
#include "buffer.h"
#include "comch_common.h"

DOCA_LOG_REGISTER(RING);

int setup_dma_ring(struct objects *objs, size_t size)
{
    doca_error_t result;
    struct dma_ring *ring;

    if (objs->dma_ring == NULL) {
        objs->dma_ring = (struct dma_ring *)malloc(sizeof(struct dma_ring));
    }

    ring = objs->dma_ring;
    ring->size = size;
    ring->producer_seq = 0;
    ring->observed_consumer_seq = 0;
    ring->descs = NULL;
    ring->consumer_state = NULL;

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_buffer_and_set_mmap(&ring->mmap, objs->dev,
                           (void **)&ring->descs, 
                           ring->size * sizeof(struct dma_desc),
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        return result;
    }

    result = alloc_buffer_and_set_mmap(&ring->consumer_mmap, objs->dev,
                           (void **)&ring->consumer_state,
                           sizeof(struct dma_ring_consumer_state),
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA consumer state: %s", doca_error_get_descr(result));
        destroy_mmap_and_free_buffer(ring->mmap, ring->descs);
        free(objs->dma_ring);
        return result;
    }
    
    /* export mmap to DPU */
    result = export_mmap_to_remote(objs, ring->mmap, 
                                   ring->descs, 
                                   ring->size * sizeof(struct dma_desc), 
                                   DMA_RING, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        destroy_mmap_and_free_buffer(ring->consumer_mmap, ring->consumer_state);
        destroy_mmap_and_free_buffer(ring->mmap, ring->descs);
        return result;
    }

    result = export_mmap_to_remote(objs, ring->consumer_mmap,
                                   ring->consumer_state,
                                   sizeof(struct dma_ring_consumer_state),
                                   DMA_RING_CONSUMER_STATE, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export DMA consumer state to DPU: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        destroy_mmap_and_free_buffer(ring->consumer_mmap, ring->consumer_state);
        destroy_mmap_and_free_buffer(ring->mmap, ring->descs);
        return result;
    }

    ring->consumer_state->consumer_seq = 0x1234; /* for testing */

    return 0;
}

struct dma_desc *get_dma_desc_for_seq(struct dma_ring *ring, uint64_t producer_seq)
{
    return ring->descs + (producer_seq % ring->size);
}

bool dma_ring_has_free_slot(const struct dma_ring *ring)
{
    return ring->producer_seq - ring->observed_consumer_seq < ring->size;
}

void dma_ring_refresh_consumer(struct dma_ring *ring)
{
    /*
     * Assumes the host CPU observes DPA PCI writes to this mmap after the DPA
     * calls __dpa_thread_window_writeback(). If the platform is not coherent,
     * a DOCA/platform-specific host invalidation step must be added here.
     */

    ring->observed_consumer_seq =
        __atomic_load_n(&ring->consumer_state->consumer_seq, __ATOMIC_ACQUIRE);
}
