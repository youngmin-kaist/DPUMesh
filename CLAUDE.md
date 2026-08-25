# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

DPUMesh offloads the host↔service-mesh-proxy hop of a Linkerd data plane onto a
BlueField-3 DPU, replacing the TCP path between the host application and the
`linkerd2-proxy` with a PCIe **DMA** transport built on NVIDIA DOCA. The host
runs a thin shim (this repo, `HOST_MODE`); the DPU runs the proxy plus the DMA
datapath (this repo, `DPU_MODE`). The two ends are the **same binary** —
`#ifdef DOCA_ARCH_DPU` in `main.c` picks the mode at compile time.

Two subprojects at the repo root:
- `DPUMesh/` — the C DOCA datapath program (`dpumesh`). This is the main work.
- `linkerd2-proxy/` — a git submodule (Rust), a fork carrying a `dmesh_doca`
  transport crate + `doca` cargo features that plug the DMA path into
  Linkerd's outbound stack. Built and run separately on the DPU.

## Build & run (DOCA C program)

Build system is **meson + ninja**; DPA device code is compiled by a separate
`dpacc` pass invoked from `meson.build` via `build_dpacc.sh`.

```bash
cd DPUMesh
meson setup build            # first time only
ninja -C build               # rebuilds host code AND re-runs dpacc on device/dpa_kernel.c
./build/dpumesh ...          # run
```

There is no test suite and no linter; correctness is validated by running the
benchmark modes end-to-end on the testbed. Requires DOCA (installed at
`/opt/mellanox/doca`), a BlueField-3, and matching PCI functions — it does not
build or run meaningfully off the testbed.

## Build & run (Rust proxy) — and how the two builds are coupled

`linkerd/doca/build.rs` **compiles the DPUMesh C sources directly into the
proxy** (`cc` on `../../../DPUMesh/{buffer,comch_*,common,dma,dpa,object,ring}.c`
plus `src/shim.c`) and statically links `DPUMesh/build/device/dpa_kernel.a`.
Consequences:

- `ninja -C DPUMesh/build` must have run at least once (and after any
  `device/dpa_kernel.c` edit) or the proxy link fails on the missing archive.
- Editing any shared C file changes **both** binaries; `cargo` reruns the shim
  build automatically (the `rerun-if-changed` list in `build.rs`).
- `main.c`, `config.c`, `dpu_worker.c`, `host_worker.c` are **not** in the shim
  build — the proxy drives the same infra through `shim.c`, not `dpu_worker.c`.
  Adding a new `.c` to `meson.build` does not add it to the proxy; update both.

```bash
cd linkerd2-proxy
cargo build --release -p linkerd2-proxy    # `doca` is a DEFAULT feature
cargo build --release -p linkerd2-proxy --no-default-features  # stock proxy, no DMA
```

Always benchmark `--release` (see gotchas). The proxy reads its DOCA PCI
addresses from `LINKERD2_PROXY_DOCA_DEV_PCI_ADDR` (`03:00.1`) and
`LINKERD2_PROXY_DOCA_REP_PCI_ADDR` (`94:00.1`) — see `linkerd2-proxy/src/main.rs`.

### The two routers — no-tower baselines

Two standalone data planes serve DMA connections without any of linkerd's L7
machinery, for measuring what the proxy's stack costs on an identical
transport. Both drive the *same* C datapath (`shim.c` + `comch_server.c` + the
DPA/DMA sources) and take the same `DMESH_ROUTER_*` environment variables, so
only the HTTP engine differs. Measured with `h2load -c1 -m100 -n20000`, h2 on
both legs, 1 core: **`dmesh-router-cpp` ~100k req/s (jemalloc + no-copy headers; ~68k before), `dmesh-router` ~31k,
DMA linkerd2-proxy ~16.6k**.

- `dmesh-router-cpp/` — C++ + libnghttp2, standalone meson project. See
  `dmesh-router-cpp/README.md`. HTTP/2 backend leg only.
- `linkerd2-proxy/dmesh-router/` — Rust + hyper, member of the proxy workspace
  (details below). HTTP/1.1 or HTTP/2 backend leg.

They cannot run at the same time (both want `03:00.1`/`94:00.1`).

### `dmesh-router` — the no-tower baseline

`linkerd2-proxy/dmesh-router/` is a second binary in the same workspace: hyper's
h2 server on the DMA client channel, hyper's client on the DMA backend channel,
routing by flow destination — no tower stack, no discovery/policy/mTLS. Same
`dmesh-doca` driver, same vendored hyper/h2, so it isolates the proxy's L7 cost
on an identical datapath. `cargo build --release -p dmesh-router`; env knobs and
the host-side run recipe are in `dmesh-router/README.md`. Unlike the proxy it has
tests (`cargo test -p dmesh-router`), which run over in-memory pipes and need no
hardware.

### Local proxy harness (`linkerd2-proxy/scripts/`)

Running the proxy outside Kubernetes needs a mock control plane; the scripts
wire it up:

- `dev-proxy-env.sh` — `source` it to export identity trust anchors/token from
  `linkerd/app/integration/src/data`, the mock svc addrs (identity `:8088`,
  destination `:8089`, policy `:8087`), outbound listener `:4140`, and the DOCA
  PCI vars.
- `run-local-mock-services.sh` — starts `mock-identity` / `mock-destination` /
  `mock-policy` (bins in `linkerd-app-integration`).
- `run-local-outbound-h2load.sh` — full end-to-end: expects a backend on
  `127.0.0.1:8086` (nginx), starts the mocks + proxy, then runs `h2load`
  against `:4140`. Override with `H2LOAD_ARGS` / `PROXY_FEATURES`; logs land in
  `target/local-outbound-h2load/`.

Mock knobs (env vars on the mock binaries) used for experiments:
- `MOCK_POLICY_REQUIRE_ID=<identity>` — inbound policy requires that mesh-TLS
  identity (exercises the fused DMA authz gate; deny-path testing).
- `MOCK_OUTBOUND_OPAQUE=1` — outbound policy serves a **bare Opaque** protocol:
  the proxy L4-forwards with **no h2 termination** (the valid way to measure
  h2-bypass; the destination-profile `opaque_protocol` hint does NOT bypass).
- `DMESH_SRC_IP` (host bridge) — flow source IP override so authz sees a
  non-localhost client.

### Testbed & canonical run commands

Host = `192.168.100.1` (rapids4, x86); DPU = `192.168.100.2` (BF-3, where
sessions run). DOCA 3.1 both ends. The host keeps its own copy of this repo at
`~/bf-workspace` (same git history — pull + `ninja` there after changing host code).

- DPU (proxy side): `./build/dpumesh -p 03:00.1 -r 94:00.1 -t 1`
- Host (shim): `~/bf-workspace/build/dpumesh -p 94:00.1 -t 1 -d 1`

DPU DOCA devices are `03:00.0/03:00.1`; the host-side BlueField PF is
`94:00.0/94:00.1`. The reverse (response) DPA path defaults to `94:00.0`
(`DMESH_REV_PCI`; `94:00.2` is the SoC management interface and returns
"Resource Not Found"). It does **not** have to differ from the forward/comch
function: `DMESH_REV_PCI=94:00.1` works and was benchmarked end-to-end — flexio
is one process per PCI function, and comch + reverse DPA live in the *same*
host process. Use that when another tenant on the host owns `94:00.0`
(symptom: `flexio_create_prm_process ... Failed to create process` from
`setup_reverse_dpa`).

CLI flags (`config.c`): `-p/--pci-addr` (mandatory), `-r/--rep-pci` (DPU only),
`-t/--threads` (worker threads / connections), `-d/--dpu-workers` (host only:
DPU servers to spread connections over).

## Architecture

### Mode dispatch (`main.c`)
`HOST_MODE` → `run_host_workers()` (`host_worker.c`); `DPU_MODE` →
`run_dpu_workers()` (`dpu_worker.c`). Each worker thread is **shared-nothing**:
its own comch server/connection, DPA thread pool, DMA engine, and progress
engines. `-t` sets the thread count on both ends.

### Control path — comch (`comch_*.{c,h}`)
DOCA Comch is the control/handshake channel (DPU = server, host = client). It
carries `struct dmesh_flow_id` (4-tuple + workload identity + `mode`) and the
mmap-export handshake messages (`enum dmesh_msg_type`: EXPORT_METADATA,
EXPORT_DPA_COMP, EXPORT_RCV_RING). The DPU-side setup is a **state machine**,
`dmesh_doca_ctrl_advance()` in `comch_server.c` (`enum dmesh_doca_init_state`
for shared infra, `enum dmesh_conn_state` per connection) — this is what the
Rust `AsyncFd` driver mirrors over FFI. `comch_consumer.c`/`comch_producer.c`
wrap the DOCA Comch consumer/producer used as the DPA↔host completion channel.

### Data path — DMA + DPA
- **Forward (host→proxy):** host writes request bytes into an exported staging
  buffer; the DPU's per-connection DPA thread (`device/dpa_kernel.c`, running on
  the DPA processor) polls a descriptor ring and DMAs the bytes into the proxy's
  receive buffer, delivering a fused completion. `dpa.c` builds the DPA thread
  pool (`DPA_THREAD_POOL_SIZE`, one thread handed out per connection) and the
  msgq/completion plumbing; `dma.c` builds each connection's private
  `doca_dma` engine + task pool.
- **Reverse (proxy→host):** the DPU exports its `rcv_ring` + `tx_staging`; the
  host launches its **own** DPA thread (on `94:00.0`) that mirrors the forward
  datapath in reverse, pulling response bytes back. See `setup_reverse_dpa()` in
  `host_worker.c`.
- **`ring.c` / `buffer.c` / `object.c`:** the descriptor rings, exportable DMA
  buffers, and the central `struct objects` (per-worker state) + `struct
  dmesh_conn` (per-connection state) that everything hangs off of.

### Flow modes (`comch_common.h`)
`DMESH_FLOW_MODE_CLIENT` — an intercepted outbound flow the proxy serves.
`DMESH_FLOW_MODE_BACKEND` — the host is a backend provider; the proxy *connects
through* this channel instead of dialing TCP. The backend channel uses a
**push** design (plan "안 2"): no host DPA — the DPU pushes ≤8KB batches with its
plain `doca_dma` into the host's rcvbuf data ring, then chains a 16B
`struct dmesh_push_desc{seq,pos,len}` into a slot ring the host busy-polls.
Plain `doca_dma` has no size cap; only the **fused DPA producer copy caps at
128B/copy**, which is why forward+reverse chunk at 128.

### DPU worker variants (`dpu_worker.c`)
`run_dpu_worker_event_driven()` (default) registers both progress-engine
notification fds with epoll and sleeps when idle. `DPUMESH_BUSY_POLL=1` selects
`run_dpu_worker()`, the busy-poll baseline (both PEs polled in a tight loop) —
kept for benchmark comparison.

### Benchmark / bridge harnesses (host side, `host_worker.c`)
These let standard tools drive the DMA path; each is selected by an env var and
generally handles one connection then exits (**restart the host `dpumesh` per
run**):
- `DMESH_BRIDGE_PORT` → `run_host_h2_bridge`: TCP↔DMA byte bridge so `h2load`
  can benchmark HTTP/2 over DMA (libnghttp2 headers aren't on the testbed, so
  it's a transparent byte pipe, not an in-C h2 client). `h2load -c1 -m<N>`.
- `DMESH_BACKEND_CONNECT` → `run_host_backend_bridge`: the DPA-free backend
  push channel to a local backend (also uses `DMESH_DST_IP`/`DMESH_DST_PORT` as
  the service key the Rust connector looks up).
- `DMESH_DPA_BENCH_MODE` (+`_SIZE/_OPS/_THREADS/_CORES`) → `run_host_dpa_bench`:
  DPA `producer_dma_copy` microbenchmark; the DPU-side equivalent lives in
  `dpa.c` (~line 969). `DMESH_MSG_SIZE` sets host benchmark message size.

## Working notes / gotchas

- **Two host processes can't both use a host DPA** (flexio single-process-per-
  function). This constraint drives several design choices: reverse path on
  `94:00.0`, backend channel using push instead of a second host DPA.
- **Tearing down a CLIENT-mode connection segfaults the *Rust* DPU processes**
  inside `doca_pe_progress`, on the consumer-PE drain (`shim.c:215` /
  `dmesh_doca_data_clear_and_drain`, called from the driver's `run` loop). It
  fires on normal close too — after a clean h2load run `dmesh-router` dies as
  the slot tears down — so restart the DPU side per run (the host bridges
  already require that). Not an L7 bug: it still reproduces with
  `dmesh-router`'s HTTP layer disabled, the connection merely held open.
  **But `dmesh-router-cpp` survives the identical teardown** (same C sources,
  same `Released DPA pool thread` / 512 `IO_FAILED` drain sequence, still
  running afterwards), so it is a latent fault in the shared teardown path
  whose manifestation depends on the host process — the Rust binaries use
  jemalloc, the C++ one glibc malloc. Suspect a use-after-free/double-free in
  the slot teardown rather than a hard null deref. BACKEND-mode teardown is
  clean in both — only the forward-DPA/consumer slot is affected.
- Editing `device/dpa_kernel.c` requires a `ninja` rebuild (dpacc reruns); DPA
  device code uses the `doca_dpa_dev_*` API and `dpaintrin.h`, a different world
  from host DOCA code.
- Build type matters enormously for the Rust proxy: **release vs debug is ~10×**
  on throughput; always benchmark the release proxy.
- The linkerd2-proxy fork gates DMA support behind cargo feature `doca` and
  uses `tikv-jemallocator` on `aarch64` (a local patch — upstream only enables
  jemalloc on x86_64, worth ~16% on the DPU).
