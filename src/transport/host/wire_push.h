#ifndef WIRE_PUSH_H
#define WIRE_PUSH_H

/*
 * Host view of the DPUMesh transport. This is the only header that both the
 * carrier (src/core) and the DPUMesh transport sources are visible from; it
 * exposes no DOCA or DPUMesh types, so the carrier compiles against the public
 * core alone.
 *
 * Two wires share this interface, selected per device by DPUMESH_WIRE:
 *   push (default)  DPU -> host bytes are pushed by the DPU's DMA engine into
 *                   the connection's window (slot ring + data ring); no host DPA
 *                   and no doorbell for the data, so the consumer polls the window
 *   pull            the mirror of the forward path: a host-owned DPA thread per
 *                   connection polls the DPU's descriptor ring and copies
 *                   tx_staging into the window's data area. DPUMESH_REV_PCI names
 *                   the host PF that runs the DPA process (its vhca needs a DPA
 *                   EU partition); DPUMESH_REV_DEV optionally extends that
 *                   process to an SF (ibdev name) that then owns the DPA objects
 */

#include <stddef.h>
#include <stdint.h>

struct wire_dev;  /* Forward declaration: one Comch device (+ DPA device on the pull wire) */
struct wire_mem;  /* Forward declaration: one registered, PCI-exported memory region */
struct wire_conn; /* Forward declaration: one Comch connection with its rings */

#define WIRE_PUSH_DESC_N   128u             /* Reverse batches a window can hold */
#define WIRE_PUSH_DATA_OFF 4096u            /* Data ring offset inside a window */
#define WIRE_PUSH_WINDOW   (1024u * 1024u)  /* One connection's receive window */
#define WIRE_DESC_MAX      8064u            /* Forward descriptor bytes */
#define WIRE_DESC_ALIGN    128u             /* Forward descriptor alignment */

#define WIRE_MODE_CLIENT_PULL  0u /* DPUMesh CLIENT: client flow, reverse by the host DPA */
#define WIRE_MODE_BACKEND      1u /* DPUMesh BACKEND: server flow, reverse pushed by the DPU */
#define WIRE_MODE_INGRESS_PUSH 2u /* DPUMesh INGRESS_PUSH: client flow, reverse pushed by the DPU */
#define WIRE_MODE_BACKEND_PULL 3u /* DPUMesh BACKEND_PULL: server flow, reverse by the host DPA */

struct wire_conn_config {
	const char *server;     /* Comch server name */
	const char *workload;   /* Workload label carried in the flow identity */
	uint32_t src_ip;        /* Network byte order */
	uint32_t dst_ip;        /* Network byte order */
	uint16_t src_port;      /* Host order, as the DPU expects */
	uint16_t dst_port;      /* Host order, as the DPU expects */
	uint32_t mode;          /* WIRE_MODE_* */
	struct wire_mem *tx;    /* Shared registered TX pool */
	struct wire_mem *rx;    /* Registered RX region */
	size_t rx_offset;       /* This connection's window inside rx */
};

/* Device: opens the Comch device (and, on the pull wire, the DPA process) */
int wire_dev_open(const char *pci, struct wire_dev **out);
void wire_dev_close(struct wire_dev *dev);
/* Nonzero when the device runs the pull wire (host DPA reverse path) */
int wire_dev_pull(const struct wire_dev *dev);

/* Memory: allocates, registers and PCI-exports `bytes`; the buffer is owned by mem */
int wire_mem_alloc(struct wire_dev *dev, size_t bytes, struct wire_mem **out);
void *wire_mem_base(const struct wire_mem *mem);
void wire_mem_free(struct wire_mem *mem);

/* Connection: Comch handshake, forward ring, reverse path setup */
int wire_conn_open(struct wire_dev *dev, const struct wire_conn_config *cfg, struct wire_conn **out);
/* Graceful Comch disconnect; safe after the peer is gone */
void wire_conn_close(struct wire_conn *conn);
/* Progresses the control path (and the reverse completions on the pull wire).
 * Returns nonzero once the DPU dropped the connection */
int wire_conn_progress(struct wire_conn *conn);

/* Doorbells: one notification fd per progress engine of the connection (control
 * path, producer, and on the pull wire the reverse completions). Returns the
 * count written to fds. The fds stay valid until wire_conn_close */
#define WIRE_CONN_FDS 3
int wire_conn_fds(struct wire_conn *conn, int *fds, int max);
/* Arms every engine so its fd signals the next completion (one-shot) */
int wire_conn_arm(struct wire_conn *conn);
/* Acknowledges a signalled fd; mandatory before that engine signals again.
 * The fd must be readable: the call blocks otherwise */
void wire_conn_clear(struct wire_conn *conn, int fd);

/* Forward path: descriptors over the TX pool */
uint32_t wire_conn_ring_free(const struct wire_conn *conn);
/* Posts one forward descriptor; returns its ticket (the ring head after it) */
uint64_t wire_conn_post(struct wire_conn *conn, uint64_t addr, uint32_t bytes);
/* Tickets consumed so far by the DPU's DPA (custody ACK) */
uint64_t wire_conn_consumed(const struct wire_conn *conn);

/* Reverse path: next landed batch, pos/len relative to the window's data ring.
 * Returns 1 with a batch, 0 when none, -1 on a malformed batch */
int wire_conn_rx_next(struct wire_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len);
/* Publishes the consumption cursor: every batch up to `seq` is released */
void wire_conn_rx_consumed(struct wire_conn *conn, uint64_t seq, uint64_t bytes);

#endif // WIRE_PUSH_H
