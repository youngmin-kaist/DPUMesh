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
    DMESH_MSG_EXPORT_METADATA = 1,
    DMESH_MSG_EXPORT_DPA_COMP = 2,
};

/* Single control-path message carrying all host-side DMA metadata: the DMA
 * ring, the send buffer and the receive buffer (address, size and PCI export
 * descriptor for each). Sent once by the host after all three are allocated. */
struct dmesh_export_metadata_msg {
    enum dmesh_msg_type type;
    /* DMA ring */
    void *ring_buf;
    size_t ring_buf_len;
    size_t ring_desc_len;
    /* send/receive DMA buffers */
    void *sndbuf;
    void *rcvbuf;
    size_t sndbuf_len;
    size_t rcvbuf_len;
    size_t snd_desc_len;
    size_t rcv_desc_len;
    uint8_t ring_desc[512];
    uint8_t snd_desc[512];
    uint8_t rcv_desc[512];
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
        struct dmesh_export_metadata_msg export_metadata_msg;
        struct dmesh_dpa_comp_msg dpa_comp_msg;
    };
};

doca_error_t
export_dma_metadata(struct objects *objs);
doca_error_t
process_export_metadata_msg(struct objects *objs, struct dmesh_export_metadata_msg *metadata_msg);
doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg);
#endif // COMCH_COMMON_H
