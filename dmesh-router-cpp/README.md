# dmesh-router-cpp

The HTTP/2 router on the DPUMesh DMA datapath, in C++ on **libnghttp2** —
a functional twin of the Rust/hyper `linkerd2-proxy/dmesh-router`.

Both binaries occupy the same position: they run on the DPU, accept connections
arriving over PCIe DMA from the host, terminate HTTP/2, and forward each request
to a backend the host provides over a second DMA channel. They drive the *same*
C datapath (`shim.c` + `comch_server.c` + the DPA/DMA sources), so the only
variable between them is the HTTP engine and its runtime — which is the point:
it isolates what the h2 stack costs on an identical transport.

```
h2load ──TCP──▶ host bridge ──forward DMA──▶ ┌───────────────────────────┐
                (DMESH_BRIDGE_PORT)          │ dmesh-router-cpp (DPU)    │
       ◀──TCP── host bridge ◀──reverse DMA── │  nghttp2 server session   │
                                             │       ↓ route(flow dst)   │
nginx ◀─TCP─ host backend bridge ◀──push DMA─│  nghttp2 client session   │
             (DMESH_BACKEND_CONNECT)         └───────────────────────────┘
```

No C-side changes are needed: the host bridges in `DPUMesh/host_worker.c` are
the same ones the Rust router uses.

## How it maps onto the datapath

| datapath | nghttp2 |
| --- | --- |
| `dmesh_doca_conn_recv_pop` → segment in the recv staging region | `nghttp2_session_mem_recv` straight out of that region (no copy) |
| tx staging + `dmesh_doca_conn_send_staged` | `nghttp2_session_send` with a send callback that copies into staging and returns `NGHTTP2_ERR_WOULDBLOCK` when it is full |
| `dmesh_doca_conn_state_get` per slot | create/destroy a session (client flow → server session, backend flow → client session) |

`TxRing` in `src/dmesh.hpp` is the C++ counterpart of the write side of
`linkerd/doca/src/io.rs` — cumulative write/publish cursors over the mapped
region, wrap-aware, unpublished bytes never overwritten.

Each request is a `Stream` bridging one server-session stream to one
client-session stream; response bodies stream back through an
`NGHTTP2_ERR_DEFERRED` data provider that is resumed as backend DATA arrives.
The event loop (`src/main.cpp`) mirrors `run_dpu_worker_event_driven()` in
`DPUMesh/dpu_worker.c`: arm both progress engines, drain control, drain data
with a budget, advance the state machine, pump the sessions, sleep on the two
notification fds with a 1 ms cap.

## Configuration

Same environment variables as the Rust router (they are never run at the same
time — both want the same PCI function):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DMESH_ROUTER_DEV_PCI` | `03:00.1` | DOCA device on the DPU |
| `DMESH_ROUTER_REP_PCI` | `94:00.1` | Representor of the host function |
| `DMESH_ROUTER_SERVER` | `DPUMesh0` | Comch server name |
| `DMESH_ROUTER_ROUTES` | *(empty)* | `authority=ip:port,...`; overrides the flow destination |
| `DMESH_ROUTER_DEFAULT_BACKEND` | *(unset)* | Fallback when the destination has no channel |
| `DMESH_ROUTER_BACKEND_WAIT_MS` | `5000` | How long a request waits for its backend channel |
| `DMESH_ROUTER_MAX_STREAMS` | `1000` | `SETTINGS_MAX_CONCURRENT_STREAMS` |
| `DMESH_BUSY_POLL` | *(unset)* | Poll the progress engines instead of sleeping on their fds |

`DMESH_ROUTER_BACKEND_PROTO` accepts only `h2` here (nghttp2 is an HTTP/2
library; there is no HTTP/1.1 backend leg). Use the Rust router for `http1`.

Routing order: `DMESH_ROUTER_ROUTES[authority]` → the connection's flow
destination → `DMESH_ROUTER_DEFAULT_BACKEND`. No channel after the wait → `503`;
backend failure → `502`.

## Build

```bash
ninja -C ../DPUMesh/build      # dpa_kernel.a must exist first
meson setup build
ninja -C build                 # -> build/dmesh-router-cpp
```

The testbed has `libnghttp2.so.14` but no `-dev` package, so the build falls
back to the headers vendored under `hpack-h2-bench/c/nghttp2/` and links the
versioned SONAME directly (same trick as `hpack-h2-bench/c/build.sh` and
`linkerd/http/nghttp2/build.rs`). If a real `libnghttp2` pkg-config file ever
appears, it is used instead.

## Run (testbed)

Identical to the Rust router — see `linkerd2-proxy/dmesh-router/README.md` for
the host-side commands. In short:

```bash
# DPU
./build/dmesh-router-cpp

# host, backend bridge first, then the h2load ingress
DMESH_BACKEND_CONNECT=127.0.0.1:8086 DMESH_DST_IP=10.0.0.1 DMESH_DST_PORT=8086 \
  ~/bf-workspace/build/dpumesh -p 94:00.1 -t 1 -d 1
DMESH_BRIDGE_PORT=8080 DMESH_REV_PCI=94:00.1 DMESH_DST_IP=10.0.0.1 DMESH_DST_PORT=8086 \
  ~/bf-workspace/build/dpumesh -p 94:00.1 -t 1 -d 1

h2load -c1 -m100 -n20000 http://127.0.0.1:8080/
```

The host bridges are single-connection and must be restarted per run. This
binary itself survives teardown and can serve run after run — unlike the Rust
router, which the shared datapath's teardown fault kills (see CLAUDE.md
gotchas). Kill it before starting the Rust router: both want `03:00.1`, and a
leftover process makes the other fail with `DOCA_ERROR_CONNECTION_ABORTED`.

## Measured (2026-08-12, testbed, 1 core, nginx `listen 8086 http2`)

`h2load -c1 -m100 -n20000`, one client channel + one backend channel, h2 on both
legs, 20000/20000 → 200:

| data plane | req/s | mean request |
| --- | --- | --- |
| **dmesh-router-cpp** (optimized: no-copy NvList + jemalloc) | **98,434 / 102,928** | ~0.9 ms |
| dmesh-router-cpp, code opts only (`-Duse_jemalloc=false`) | 82,124 | — |
| dmesh-router-cpp, first version | 69,187 / 67,835 | 1.43 ms |
| dmesh-router (hyper) | 30,138 / 31,483 | 3.30 ms |
| DMA linkerd2-proxy (full tower stack) | ~16,600 | — |

Same host bridges, same DMA transport, same nginx — so the spread is the L7
engine and its runtime, not the datapath.

### Where the cycles went (perf, 500k requests, DWARF call stacks)

Profiling the first version (self-time by DSO): **libnghttp2 43%** (HPACK/
Huffman — intrinsic), **libc 28%** (nearly all `_int_malloc`/`_int_free`/
`memcpy` driven by per-request `std::string` churn), our code 13%, libstdc++ 5%
(`basic_string::compare` from header-name matching). The router logic itself
(`submit`/`respond`/routing) was ~5% — the allocator traffic was the target:

- **NvList no longer copies** — it points at Stream-owned strings/literals;
  nghttp2 copies them itself inside submit, so the deque copy was pure churn.
  Header-name matching now uses length+`memcmp` against literals instead of
  constructing `std::string`s (+20% alone).
- **jemalloc interposed** at link time (`libjemalloc.so.2`, no -dev package
  needed), matching the Rust binaries' tikv-jemallocator (+22% more).

The Rust router's profile (in `../linkerd2-proxy/dmesh-router/README.md`) is
the other half of the comparison: its h2 cost is similar (36% vs 43% here), but
~22% of its cycles go to atomics + tokio — layers that don't exist in this
callback-driven design, which is the structural source of the ~3× gap.
