#ifndef RING_H
#define RING_H

#include <stdint.h>
#include <stddef.h>
#include <doca_error.h>

#define DMA_RING_SIZE 1024

struct dma_desc;
struct dma_ring_ctrl;
struct doca_mmap;
struct objects;

struct dma_ring {
    struct doca_mmap *mmap;
    uint64_t head;
    uint64_t tail;
    uint32_t size;
    void *buffer;
    struct dma_ring_ctrl *ctrl;
    struct dma_desc *descs;
};

struct doca_dev;

int setup_dma_ring(struct objects *objs, size_t size);

/* Reverse-path ring helpers: allocate/free a standalone ring on an explicit
 * device (the DPU-owned rcv_ring the host DPA thread polls). */
doca_error_t alloc_dma_ring(struct dma_ring **ring_out, struct doca_dev *dev, size_t size);
void free_dma_ring(struct dma_ring *ring);

struct dma_desc *get_next_dma_desc(struct dma_ring *ring);
void commit_dma_desc(struct dma_ring *ring);
#endif /* RING_H */
