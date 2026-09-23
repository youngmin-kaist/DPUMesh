# Host library over the DPUMesh transport

The host library (`libdpumesh.so.5`) is the DPUmesh native API and core; the
DPU is the unmodified DPUMesh data plane and its embedded Linkerd proxy. The two
meet at DPUMesh's host wire: one Comch connection per stream, a forward
descriptor ring per connection and, back to the host, either the DPA-free push
channel (`DPUMESH_WIRE=push`, the default) or the pull wire
(`DPUMESH_WIRE=pull`): a host-owned DPA thread per connection that mirrors the
forward path, polling the DPU's descriptor ring and copying its tx_staging
into the connection's window (flow modes CLIENT / BACKEND_PULL, so the DPU
exports rcv_ring + tx_staging instead of pushing).

## Layout

- `include/dpumesh`: public API and descriptor layout (ABI 5).
- `src/core/dmesh_core.c`: channels, EQs, QPs, TX reservation and credits,
  custody ACK reclamation, RX delivery and the accept queue, FIN and teardown,
  the EQ readiness fd (doorbells, fallback tick, spin window).
- `src/core/native_transport.h`: the private carrier contract
  (open, connect, submit, poll, release, wait, resolve, disconnect, close).
- `src/core/carrier_push.c`: the carrier over the DPUMesh wire.
- `src/transport/host/wire_push.[ch]`: the wire layer, the only host-library
  files that include DOCA headers; they call the transport's host sources
  (`src/transport/{common,host}/*.c`) for device open, Comch client and
  producer setup, ring setup and the export message. `src/core` includes only
  `wire_push.h`, which exposes no DOCA types.
- `src/transport/host/wire_host_stubs.c`: two server-only symbols the shared
  transport sources reference but a host never executes.
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

### Readiness: no background thread

The library creates no thread of its own. The EQ thread that calls
`dmesh_poll_eq` drains every stripe in line (`dpumesh_eq_drain`) and
publishes its QPs' retained transmit tails, and `dmesh_eq_fd` hands out an
epoll set that wakes it: its eventfd (deliveries from other EQ threads,
accepts), a one-shot timerfd programmed to the earliest retained-tail
deadline, the doorbells of the stripes it owns, the doorbells of the spare
backend flows, and a fallback tick. A stripe's doorbell is the
carrier's per-slot epoll of the wire's progress-engine notification fds
(Comch control path, producer, and on the pull wire the reverse msgq
completions); the core moves it from the spare set to the owning EQ at
connect/accept and back at free.

Two things have no doorbell: custody ACKs (the DPU's DPA writes
`consumer_head` into host memory) and push-wire batches (the DPU's DMA engine
writes the window). While a sleeping EQ has either outstanding, a periodic
timerfd (`DPUMESH_TICK_US`, default 50) polls for it; an idle pull-wire
process runs no timer at all (measured 0.4% CPU for an idle server).

An armed completion queue raises a hardware event per completion, so an EQ
that runs empty first spins for `DPUMESH_SPIN_US` (default 1000): it signals
its own eventfd and leaves it unread, so the caller's sleep returns at once and
it polls again; only an EQ empty for the whole window acknowledges its
doorbells and arms them before the real sleep. With the window at 50 us the
2-flow 8 KiB echo lost 8% to wake latency; at 1 ms it gains on the old drain
threads (20.4 vs 18.9 Gbps, 64 B RTT 26.4 vs 27.0 us) with fewer threads and
less host CPU (4 flows: 197% vs 255%).

### Pull wire

`DPUMESH_REV_PCI` names the host function whose DPA runs the reverse threads
(default `0b:00.0`); it must differ from the Comch function (flexio allows one
process per function) and its vhca needs a DPA EU partition on the DPU
(`dpaeumgmt partition create --vhca_list 0 --range_eus 0-63`, or
`scripts/assign_dpa_eu.sh` for SF groups). Each connection imports the DPU's
exports on that device, gets one DPA thread + msgq (its own progress engine)
and the same `poll_desc_ring` kernel the DPU runs forward; completions arrive
as msgq messages and the application's releases feed the kernel's staging gate
(`rd_pos`, published every 64 KiB). The dpacc host stub is compiled with
`-fPIC` so `dpa_kernel.a` links into the shared library (root `Makefile`).
A host SF cannot create the DPA process itself (refused by the firmware), but
`DPUMESH_REV_DEV=<ibdev of the SF>` runs the official extended-context flow:
the process is created on the PF (`DPUMESH_REV_PCI`), `doca_dpa_device_extend`
extends it to the SF, every DPA object (thread, completions, msgqs, mmap
handles, buf_arr) is created on the SF, and the kernel switches to it with
`doca_dpa_dev_device_set` (the thread argument's `dpa_dev`, also passed to the
init RPC; without the switch nothing activates). Verified 2026-09-24 at the
PF's speed (1 flow 10.7 Gbps, 4 flows 32 Gbps, 64 B RTT 25 µs); the SF needs
no EU partition of its own, only the PF's vhca does. Two firmware-level
observations on this node: several host processes can each hold a DPA
process on the same PF (and share one SF), but a second *distinct* SF
extended at the same time fails its consumer-completion CQ (devx syndrome
0x5ecb3) whatever the partition layout. So today a per-pod deployment is
"SF for Comch, DPA on the PF" (or one shared SF); one SF per pod with its own
DPA objects waits on that firmware limit.

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

All values come from the environment; `.env.example` lists them with
placeholders. `DPUMESH_PCI_ADDR`, `DPUMESH_SERVER` (default `DPUMesh0`), `DPUMESH_CONFIG`
(registry, default `/etc/dpumesh/registry`), `DPUMESH_POD_IP`,
`DPUMESH_WORKLOAD`, `DPUMESH_POD_ID` (default 0), `DPUMESH_SERVICE` for a
server, `DPUMESH_BACKEND_POOL` (spare flows, default 8), `DPUMESH_BACKEND_MAX`
(default 16). `DPUMESH_SPIN_US` is the empty-poll window before an EQ arms
its doorbells and `DPUMESH_TICK_US` the fallback poll period while doorbell-less
traffic is outstanding. `DPUMESH_CARRIER_TRACE` and `DPUMESH_CORE_TRACE` print flow,
descriptor and event traces to stderr.

## Build

```sh
make lib            # host library and preload shim
make test           # host-only checks and ABI contract
make examples       # native and preload examples; see examples/README.md
cd src/transport && meson setup build && meson compile -C build   # transport, on the DPU
```
