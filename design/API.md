# Native API

The native interface in `include/dpumesh/dmesh.h` has nineteen calls and ABI
major 5. It describes
reliable full-duplex byte streams using channels, event queues (EQs), queue
pairs (QPs), and registered buffers. It exposes no remote-memory operations.
The façade and channel/QP core are linked into `libdpumesh.so.5`. The private
carrier maps their operations to privileged direct DOCA registration.

## Objects and ownership

A channel is the registration and memory domain. Each polling thread owns an
EQ; each QP belongs to one EQ. Construct channel, EQ, then QP and destroy them
in reverse order. The public lifecycle contract rejects destruction of parents
with live children. A QP's transmit calls form a serial stream; the caller
serializes them against QP destruction. `user_data` belongs to the application.

| Surface | Calls |
|---|---|
| Channel | `dmesh_create_channel`, `dmesh_destroy_channel`, `dmesh_pod_id`, `dmesh_msg_max`, `dmesh_post_max` |
| EQ | `dmesh_create_eq`, `dmesh_destroy_eq`, `dmesh_eq_fd`, `dmesh_eq_next_deadline_ns` |
| QP | `dmesh_create_qp`, `dmesh_destroy_qp`, `dmesh_abort_qp` |
| TX | `dmesh_alloc`, `dmesh_post_send`, `dmesh_flush`, `dmesh_tx_inflight` |
| Events | `dmesh_poll_eq`, `dmesh_release_rx_buffer` |
| Statistics | `dmesh_get_tx_stats` |

## Transmit and receive

`dmesh_alloc` acquires the QP's transmit gate and reserves contiguous memory.
A successful `dmesh_post_send` commits that reservation and releases the gate;
it preserves the core's errno on failure. Allocation pressure invokes the
core's tail-pressure handling before returning `EAGAIN`. Submission, completion
and buffer reuse are distinct operations.

`dmesh_poll_eq` assists the drain, publishes due tails, and returns connection,
receive, FIN, TX-ready and error events. It drains the accepted QP's held first
fragment before queued fragments and preserves readiness when the caller's
event array fills. A receive event holds credit until
`dmesh_release_rx_buffer`; the façade clears its token after release. FIN has
no payload lease. Process an entire returned event batch before destroying a
QP referenced by that batch.

The POSIX adapter in `src/facade/dmesh_preload.c` uses this same native surface
and core contract. Its contract tests exercise descriptor handling and syscall
semantics without registering DOCA resources. The internal header is not a
private wire format that can be sent directly to the existing DOCA rings.

## Completion and close

`dmesh_post_send` commits a reservation, not an application message boundary.
The library emits complete DMA units immediately and retains a short tail
until flush, pressure or its bounded deadline. `dmesh_flush` publishes that
tail; it does not wait for the remote application. `dmesh_tx_inflight` reports
whether submitted units remain in DPU custody. An EQ event may contain only part of a
write, and several writes can share a transfer unit.

`dmesh_destroy_qp` flushes outgoing data and initiates FIN retirement. Its
handle is no longer usable after successful destruction. The transport keeps
in-flight ownership until retirement finishes. `dmesh_abort_qp` cancels the
stream; retained RX event buffers must still be released. Parent destruction
returns `EBUSY` while its children remain. A failed device quiescence retains
registered resources so DMA cannot target freed memory.

## Event loop

A server advertises `DPUMESH_SERVICE` before channel creation. A `CONN_REQ`
event supplies the accepted QP; polling also returns its first payload.
`RECV` owns a buffer lease, `RECV_FIN` marks inbound EOF, `TX_READY` permits a
blocked allocation retry, and `TX_ERROR` reports a failed deferred write.
Neither EOF nor a readiness event owns an RX buffer.

An event loop drains `dmesh_poll_eq` until empty, then waits on `dmesh_eq_fd`.
Drain the eventfd on wake. Bound the wait by `dmesh_eq_next_deadline_ns`, which
returns `-1` when no retained tail needs service. Serialize EQ polling on one
thread and serialize each QP's reserve/post/flush/close operations.

The [complete C examples](../bench/examples/README.md) show this lifecycle.
The [verification suite](../tests/README.md) checks ABI compatibility, the real
QP/EQ core, reverse ACK order, receive leases and facade behavior. Hardware
ordering, quiescence and performance require a configured Host/DPU run.
