/* Host view of the DPUMesh push transport. This is the only file that both
 * the carrier and the DPUMesh headers are visible from; it exposes no DOCA or
 * DPUMesh types, so the carrier compiles against the public core alone. */
#ifndef DMESH_WIRE_PUSH_H
#define DMESH_WIRE_PUSH_H
#include <stddef.h>
#include <stdint.h>

#define WIRE_PUSH_DESC_N   128u
#define WIRE_PUSH_DATA_OFF 4096u
#define WIRE_PUSH_WINDOW   (1024u * 1024u)   /* one connection's rcvbuf */
#define WIRE_DESC_MAX      8064u             /* forward descriptor bytes */
#define WIRE_DESC_ALIGN    128u
#define WIRE_MODE_BACKEND      1u
#define WIRE_MODE_INGRESS_PUSH 2u

struct wire_dev;
struct wire_mem;
struct wire_conn;

struct wire_conn_config {
    const char *server, *workload;
    uint32_t src_ip, dst_ip;      /* network byte order */
    uint16_t src_port, dst_port;  /* host order, as the DPU expects */
    uint32_t mode;
    struct wire_mem *tx;          /* shared registered TX pool */
    struct wire_mem *rx;          /* registered RX region */
    size_t rx_offset;             /* this connection's window inside rx */
};

int wire_dev_open(const char *pci, struct wire_dev **out);
void wire_dev_close(struct wire_dev *);
/* Allocates, registers and PCI-exports `bytes`; the buffer is owned by mem. */
int wire_mem_alloc(struct wire_dev *, size_t bytes, struct wire_mem **out);
void *wire_mem_base(const struct wire_mem *);
void wire_mem_free(struct wire_mem *);

int wire_conn_open(struct wire_dev *, const struct wire_conn_config *, struct wire_conn **out);
/* Graceful Comch disconnect; safe after the peer is gone. */
void wire_conn_close(struct wire_conn *);
/* Progresses the control path. Returns nonzero once the DPU dropped the connection. */
int wire_conn_progress(struct wire_conn *);
uint32_t wire_conn_ring_free(const struct wire_conn *);
/* Posts one forward descriptor over the TX pool; returns its ticket. */
uint64_t wire_conn_post(struct wire_conn *, uint64_t addr, uint32_t bytes);
uint64_t wire_conn_consumed(const struct wire_conn *);
/* Next published push batch: pos/len are relative to the data ring. */
int wire_conn_rx_next(struct wire_conn *, uint64_t *seq, uint32_t *pos, uint32_t *len);
void wire_conn_rx_consumed(struct wire_conn *, uint64_t seq, uint64_t bytes);
#endif
