#ifndef COMCH_COMMON_H
#define COMCH_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <doca_error.h>
#include <doca_mmap.h>

struct objects;

enum msg_direction {
    HOST_TO_DPU = 0,
    DPU_TO_HOST = 1,
};

enum dmesh_msg_type {
    DMESH_MSG_EXPORT_BUFFER = 1,
    DMESH_MSG_EXPORT_RING = 2,
    DMESH_MSG_EXPORT_DPA_COMP = 3,
};

enum mmap_type {
    DMA_BUFFER = 1,
    DMA_RING = 2,
};

struct dmesh_export_buf_msg {
    enum dmesh_msg_type type;
    enum mmap_type mmap_type;
    void *rcvbuf;
    void *sndbuf;
    size_t rcvbuf_len;
    size_t sndbuf_len;
    size_t rcv_desc_len;
    size_t snd_desc_len;
    uint8_t rcv_desc[512];
    uint8_t snd_desc[512];
};

struct dmesh_export_ring_msg {
    enum dmesh_msg_type type;
    enum mmap_type mmap_type;
    void *buf;
    size_t buf_len;
    size_t desc_len;
    uint8_t desc[512];
};

typedef uint64_t doca_dpa_dev_comch_consumer_completion_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_comch_consumer_t;

struct dmesh_dpa_comp_msg {
    enum dmesh_msg_type type;
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_comch_consumer_t dpa_consumer;
};

struct dmesh_comch_msg {
    enum dmesh_msg_type type;
    union 
    {
        struct dmesh_export_buf_msg export_buf_msg;
        struct dmesh_export_ring_msg export_ring_msg;
        struct dmesh_dpa_comp_msg dpa_comp_msg;
    };
};

doca_error_t
export_dma_buffer(struct objects *objs);
doca_error_t
export_dma_ring(struct objects *objs);
doca_error_t
export_mmap_to_remote(struct objects *objs, struct doca_mmap *mmap, void *buffer, size_t buf_size, enum mmap_type mmap_type, enum msg_direction direction);
doca_error_t
process_export_ring_msg(struct objects *objs, struct dmesh_export_ring_msg *export_ring_msg);
doca_error_t
process_export_buf_msg(struct objects *objs, struct dmesh_export_buf_msg *export_buf_msg);
doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg);
#endif // COMCH_COMMON_H
