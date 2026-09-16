/* Private native carrier boundary. Public API layouts do not depend on DOCA. */
#ifndef DMESH_NATIVE_TRANSPORT_H
#define DMESH_NATIVE_TRANSPORT_H
#include "dmesh_core.h"
struct dmesh_native_transport;
struct dmesh_native_config {
    size_t bytes;
    /* K is the Host-to-DPU forward-ring count. L is the independently
     * owned DPU-to-Host landing-stripe count. */
    int slot_size, rings, rx_stripes;
    const char *service_name;
    void *tx, *rx;
    size_t rx_bytes;              /* receive region size; 0 means the TX size */
    int pod_id, service_id, stripes;
};
enum { DMESH_NATIVE_RX = 1, DMESH_NATIVE_ACK, DMESH_NATIVE_ERROR };
struct dmesh_native_event {
    int kind, error;
    sw_descriptor_t desc;
    uint16_t port, seq;
    uint32_t seq_count;
};
/* open succeeds only after registered memory and remote readiness are live. */
int dmesh_native_open(struct dmesh_native_transport **, struct dmesh_native_config *);
/* close retains ownership on failure; no mapping may be freed before quiesce. */
int dmesh_native_close(struct dmesh_native_transport *);
/* submit borrows the registered TX range until a custody ACK, including FIN. */
int dmesh_native_submit(struct dmesh_native_transport *, const sw_descriptor_t *);
/* A stripe has one consumer at a time; the core provides per-stripe exclusion. */
int dmesh_native_poll(struct dmesh_native_transport *, int stripe, struct dmesh_native_event *);
void dmesh_native_release(struct dmesh_native_transport *, int byte_offset);
void dmesh_native_wait(struct dmesh_native_transport *, int shard, int shards, int timeout_ms);
int dmesh_native_resolve(struct dmesh_native_transport *, const char *name, uint32_t addr, uint16_t port);
/* A client port's stream exists from connect until its FIN or disconnect. */
int dmesh_native_connect(struct dmesh_native_transport *, uint16_t port, int service_id);
int dmesh_native_disconnect(struct dmesh_native_transport *, uint16_t port);
#endif
