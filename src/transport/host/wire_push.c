/* DPUMesh push transport, host end. Uses the unmodified DPUMesh host sources:
 * Comch client and producer setup, the forward descriptor ring, buffer
 * registration and the single export message. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "wire_push.h"
#include "object.h"
#include "common.h"
#include "comch_client.h"
#include "comch_producer.h"
#include "comch_common.h"
#include "buffer.h"
#include "ring.h"
#include "dma.h"
#include "dpa_common.h"
#include <doca_log.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

_Static_assert(WIRE_PUSH_DESC_N == DMESH_PUSH_DESC_N, "push slot count");
_Static_assert(WIRE_PUSH_DATA_OFF == DMESH_PUSH_DATA_OFF, "push data offset");
_Static_assert(WIRE_PUSH_WINDOW == BUFFER_SIZE, "push window");

#define WIRE_RING_SIZE 1024u   /* the DPUMesh host library's forward ring depth */

struct wire_dev { struct doca_dev *dev; };
struct wire_mem { struct doca_mmap *mmap; void *buf; size_t bytes; doca_dpa_dev_mmap_t dpa; };
struct wire_conn {
    struct objects *objs;
    doca_dpa_dev_mmap_t tx_dpa;
    volatile struct dmesh_push_desc *descs;
    volatile struct dmesh_push_cursor *cursor;
    size_t data_size;
    uint64_t expected;
    int closed;
};
static int error_number(doca_error_t e)
{
    switch (e) {
    case DOCA_ERROR_NO_MEMORY: return ENOMEM;
    case DOCA_ERROR_INVALID_VALUE: return EINVAL;
    case DOCA_ERROR_AGAIN: return EAGAIN;
    case DOCA_ERROR_NOT_FOUND: return ENODEV;
    default: return EIO;
    }
}
int wire_dev_open(const char *pci, struct wire_dev **out)
{
    static int logging;
    if (!logging) { (void)doca_log_backend_create_standard(); logging = 1; }
    struct wire_dev *d = calloc(1, sizeof(*d));
    if (!d) return -1;
    doca_error_t e = open_doca_device_with_pci(pci, NULL, &d->dev);
    if (e != DOCA_SUCCESS) { free(d); errno = error_number(e); return -1; }
    *out = d; return 0;
}
void wire_dev_close(struct wire_dev *d)
{
    if (!d) return;
    if (d->dev) (void)doca_dev_close(d->dev);
    free(d);
}
int wire_mem_alloc(struct wire_dev *d, size_t bytes, struct wire_mem **out)
{
    struct wire_mem *m = calloc(1, sizeof(*m));
    if (!m) return -1;
    doca_error_t e = alloc_buffer_and_set_mmap(&m->mmap, d->dev, &m->buf, bytes,
        DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (e != DOCA_SUCCESS) { free(m); errno = error_number(e); return -1; }
    e = doca_mmap_dev_get_dpa_handle(m->mmap, d->dev, &m->dpa);
    if (e != DOCA_SUCCESS) {
        (void)destroy_mmap_and_free_buffer(m->mmap, m->buf); free(m);
        errno = error_number(e); return -1;
    }
    m->bytes = bytes; *out = m; return 0;
}
void *wire_mem_base(const struct wire_mem *m) { return m ? m->buf : NULL; }
void wire_mem_free(struct wire_mem *m)
{
    if (!m) return;
    if (m->mmap) (void)destroy_mmap_and_free_buffer(m->mmap, m->buf);
    free(m);
}
/* Mirrors the DPUMesh host library's close: stop the producer, then the
 * client, progressing their engines until idle, then release the ring. The
 * shared pool and region mmaps belong to the caller and are left alone. */
static void conn_teardown(struct wire_conn *c)
{
    struct objects *o = c->objs;
    enum doca_ctx_states st;
    int spins;
    if (o->producer) {
        (void)doca_ctx_stop(doca_comch_producer_as_ctx(o->producer));
        spins = 0;
        while (spins++ < 100000 &&
               doca_ctx_get_state(doca_comch_producer_as_ctx(o->producer), &st) == DOCA_SUCCESS &&
               st != DOCA_CTX_STATE_IDLE)
            (void)doca_pe_progress(o->producer_pe ? o->producer_pe : o->pe);
        (void)doca_comch_producer_destroy(o->producer); o->producer = NULL;
    }
    if (o->producer_mem) { clean_local_mem_bufs(o->producer_mem); free(o->producer_mem); o->producer_mem = NULL; }
    if (o->producer_pe) { (void)doca_pe_destroy(o->producer_pe); o->producer_pe = NULL; }
    if (o->cc_client) {
        (void)doca_ctx_stop(doca_comch_client_as_ctx(o->cc_client));
        spins = 0;
        while (spins++ < 100000 &&
               doca_ctx_get_state(doca_comch_client_as_ctx(o->cc_client), &st) == DOCA_SUCCESS &&
               st != DOCA_CTX_STATE_IDLE)
            (void)doca_pe_progress(o->pe);
        (void)doca_comch_client_destroy(o->cc_client); o->cc_client = NULL; o->cc_server = NULL;
    }
    if (o->dma_ring) {
        if (o->dma_ring->mmap) (void)destroy_mmap_and_free_buffer(o->dma_ring->mmap, o->dma_ring->buffer);
        free(o->dma_ring); o->dma_ring = NULL;
    }
    o->sndbuf.mmap = NULL; o->rcvbuf.mmap = NULL;   /* shared, owned by the carrier */
    o->dev = NULL;                                  /* shared, owned by the carrier */
    cleanup_objects(o);
    free(o);
    free(c);
}
int wire_conn_open(struct wire_dev *d, const struct wire_conn_config *cfg, struct wire_conn **out)
{
    struct wire_conn *c = calloc(1, sizeof(*c));
    struct objects *o = c ? calloc(1, sizeof(*o)) : NULL;
    if (!c || !o) { free(c); free(o); errno = ENOMEM; return -1; }
    c->objs = o; o->dev = d->dev;
    o->flow.src_ip = cfg->src_ip; o->flow.dst_ip = cfg->dst_ip;
    o->flow.src_port = cfg->src_port; o->flow.dst_port = cfg->dst_port;
    o->flow.mode = cfg->mode;
    snprintf(o->flow.src_workload, sizeof(o->flow.src_workload), "%s", cfg->workload ? cfg->workload : "");
    doca_error_t e = init_comch_ctrl_path_client(cfg->server, o, true);
    if (e != DOCA_SUCCESS) goto fail;
    e = init_comch_datapath_producer(o);
    if (e != DOCA_SUCCESS) goto fail;
    if (setup_dma_ring(o, WIRE_RING_SIZE) != 0) { e = DOCA_ERROR_NO_MEMORY; goto fail; }
    o->sndbuf.mmap = cfg->tx->mmap; o->sndbuf.buf = cfg->tx->buf; o->sndbuf.size = cfg->tx->bytes;
    o->rcvbuf.mmap = cfg->rx->mmap; o->rcvbuf.buf = (char *)cfg->rx->buf + cfg->rx_offset;
    o->rcvbuf.size = WIRE_PUSH_WINDOW;
    memset(o->rcvbuf.buf, 0, DMESH_PUSH_DATA_OFF);
    c->descs = (volatile struct dmesh_push_desc *)o->rcvbuf.buf;
    c->cursor = (volatile struct dmesh_push_cursor *)((char *)o->rcvbuf.buf + DMESH_PUSH_CURSOR_OFF);
    c->cursor->consumed_seq = 0; c->cursor->consumed_bytes = 0;
    c->cursor->magic = DMESH_PUSH_FC_MAGIC;
    c->data_size = WIRE_PUSH_WINDOW - DMESH_PUSH_DATA_OFF;
    c->expected = 1;
    c->tx_dpa = cfg->tx->dpa;
    e = export_dma_metadata(o);
    if (e != DOCA_SUCCESS) goto fail;
    *out = c; return 0;
fail:
    conn_teardown(c);
    errno = error_number(e); return -1;
}
void wire_conn_close(struct wire_conn *c)
{
    if (!c || c->closed) return;
    c->closed = 1;
    conn_teardown(c);
}
int wire_conn_progress(struct wire_conn *c)
{
    (void)doca_pe_progress(c->objs->pe);
    if (c->objs->producer_pe) (void)doca_pe_progress(c->objs->producer_pe);
    return c->objs->peer_gone;
}
uint32_t wire_conn_ring_free(const struct wire_conn *c)
{
    struct dma_ring *r = c->objs->dma_ring;
    uint64_t used = r->head - r->ctrl->consumer_head;
    return used >= r->size ? 0 : (uint32_t)(r->size - used);
}
uint64_t wire_conn_post(struct wire_conn *c, uint64_t addr, uint32_t bytes)
{
    struct dma_ring *r = c->objs->dma_ring;
    struct dma_desc *d = get_next_dma_desc(r);   /* the caller checked ring_free */
    d->mmap = c->tx_dpa; d->addr = addr; d->size = bytes;
    commit_dma_desc(r);
    return r->head;
}
uint64_t wire_conn_consumed(const struct wire_conn *c)
{
    return c->objs->dma_ring->ctrl->consumer_head;
}
int wire_conn_rx_next(struct wire_conn *c, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
    volatile struct dmesh_push_desc *d = &c->descs[c->expected % DMESH_PUSH_DESC_N];
    if (d->seq != c->expected) return 0;
    uint32_t p = d->pos, n = d->len;
    if (n == 0 || (size_t)p + n > c->data_size) return -1;
    *seq = c->expected; *pos = p; *len = n;
    c->expected++;
    return 1;
}
void wire_conn_rx_consumed(struct wire_conn *c, uint64_t seq, uint64_t bytes)
{
    c->cursor->consumed_seq = seq;
    c->cursor->consumed_bytes = bytes;
}
