#ifndef RING_H
#define RING_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define DMA_RING_SIZE 1024

struct dma_desc;
struct grpc_req_desc;
struct dma_ring_consumer_state;
struct doca_mmap;
struct objects;

struct dma_ring {
    struct doca_mmap *mmap;
    struct doca_mmap *consumer_mmap;
    struct dma_ring_consumer_state *consumer_state;
    uint64_t producer_seq;
    uint64_t observed_consumer_seq;
    uint32_t size;
    struct dma_desc *descs;
};

int setup_dma_ring(struct objects *objs, size_t size);

struct dma_desc *get_dma_desc_for_seq(struct dma_ring *ring, uint64_t producer_seq);
struct grpc_req_desc *get_grpc_req_desc_for_seq(struct dma_ring *ring, uint64_t producer_seq);
bool dma_ring_has_free_slot(const struct dma_ring *ring);
void dma_ring_refresh_consumer(struct dma_ring *ring);
#endif /* RING_H */
