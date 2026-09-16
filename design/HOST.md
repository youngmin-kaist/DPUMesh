# Host library over the DPUMesh transport

The host library (`libdpumesh.so.5`) is the DPUmesh native API and core; the
DPU is the unmodified DPUMesh data plane and its embedded Linkerd proxy. The two
meet at DPUMesh's host wire: one Comch connection per stream, a forward
descriptor ring per connection and the DPA-free push channel back to the host.

## Layout

- `include/dpumesh`: public API and descriptor layout (ABI 5).
- `src/core/dmesh_core.c`: channels, EQs, QPs, TX reservation and credits,
  custody ACK reclamation, RX delivery and the accept queue, FIN and teardown,
  drain threads with adaptive polling.
- `src/core/native_transport.h`: the private carrier contract
  (open, connect, submit, poll, release, wait, resolve, disconnect, close).
- `src/core/carrier_push.c`: the carrier over the DPUMesh wire.
- `src/core/wire_push.[ch]`: the only files that include DPUMesh headers; they
  call the unmodified DPUMesh host sources (`DPUMesh/*.c`) for device open,
  Comch client and producer setup, ring setup and the export message.
- `src/core/wire_host_stubs.c`: two server-only symbols the shared DPUMesh
  sources reference but a host never executes.
- `src/facade`: the native API and the POSIX preload shim.

## Mapping onto the wire

| API | Carrier |
|---|---|
| `dmesh_create_channel` | Opens the PCI device, registers one TX pool and one RX region (32 windows of 1 MiB), loads the registry. A server channel (`DPUMESH_SERVICE`) keeps `DPUMESH_BACKEND_POOL` unclaimed BACKEND flows open, each under a fresh upstream port, up to `DPUMESH_BACKEND_MAX` flows in total; a flow is replaced as soon as a stream claims it, so the DPU connector always finds a ready backend. |
| `dmesh_create_qp` | Opens an `INGRESS_PUSH` flow: source `DPUMESH_POD_IP` and the QP port, destination the registry address of the service, `DPUMESH_WORKLOAD` as identity label. |
| inbound stream | The first push batch on a BACKEND flow enters the core's accept queue under that flow's upstream port. After the stream closes, the flow reopens under a new port. |
| `dmesh_post_send` | The descriptor's TX-pool range is posted to the flow's forward ring as one or two DPUMesh descriptors (a multiple of 128 bytes plus a remainder of at most 128 bytes, each at most 8064 bytes). |
| custody ACK | The DPA's `consumer_head` passing a descriptor's ticket. |
| `dmesh_poll_eq` receive | One push batch `{seq, pos, len}` becomes one receive event pointing into the flow's data ring. No copy. |
| `dmesh_release_rx_buffer` | Marks the batch released; the consumption cursor advances over the released prefix and the DPU pulls it for flow control. A long-held batch blocks only its own QP. |
| `dmesh_destroy_qp`, `dmesh_abort_qp` | The zero-length close descriptor closes the flow; the DPU's disconnect arrives as a zero-length receive event. |

The wire has no doorbell. Drain threads use the adaptive nap and, with any flow
open, wait at most 100 us between passes.

## Limits

A DPU worker serves 32 flows (one DPA thread each), shared by client QPs and
the backend pool. Opening a QP performs a Comch handshake and takes
milliseconds. `dmesh_msg_max` is 8192; a descriptor larger than 8064 bytes is
split on the wire and reassembled by the stream.

## DPU worker mode

The DPUMesh proxy's event-driven worker (`DMESH_BUSY_POLL` unset) stops
serving a flow after an idle gap of about a second on this build; the
measured configuration runs the proxy with `DMESH_BUSY_POLL=1`. The cause is
in the proxy's notification handling, not in this library.

## Configuration

`DPUMESH_PCI_ADDR`, `DPUMESH_SERVER` (default `DPUMesh0`), `DPUMESH_CONFIG`
(registry, default `/etc/dpumesh/registry`), `DPUMESH_POD_IP`,
`DPUMESH_WORKLOAD`, `DPUMESH_POD_ID` (default 0), `DPUMESH_SERVICE` for a
server, `DPUMESH_BACKEND_POOL` (spare flows, default 8), `DPUMESH_BACKEND_MAX`
(default 16). `DPUMESH_DRAIN_NAP_US` and `DPUMESH_DRAIN_NAP_CAP_US` bound the
polling interval. `DPUMESH_CARRIER_TRACE` and `DPUMESH_CORE_TRACE` print flow,
descriptor and event traces to stderr.

## Build

```sh
make lib            # host library and preload shim
make test           # host-only checks and ABI contract
make examples       # hello_dpumesh, hello_dpumesh_server
cd DPUMesh && meson setup build && meson compile -C build   # DPU side, on the DPU
```
