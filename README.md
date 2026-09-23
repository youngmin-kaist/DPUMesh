# DPUMesh

| Directory | Side | Contents |
|---|---|---|
| `src/transport/` | both | The C DOCA transport, a Meson project and the only tree that includes DOCA headers: `common/` (both sides), `dpu/`, `host/`, `device/` (DPA kernel), `legacy/` (bridges, old Go lib), `apps/` (the `dpumesh` CLI). Built into static archives that the proxy, the C++ router and the host library link. |
| `linkerd2-proxy/` | DPU | The linkerd2-proxy fork with the DMA transport (git submodule). Its build links `src/transport/build/libdmesh_*.a`, so the two directories stay side by side. |
| `include/`, `src/core`, `src/facade` | host | The DPUmesh host library, DOCA-free: public API (`include/dpumesh`), core and carrier (`src/core`), native API and POSIX preload façades (`src/facade`). |
| `integrations/grpc/` | host | C++ and Go gRPC adapters over the host library. |
| `examples/` | host | One working example per API: native, preload, gRPC. |
| `tests/` | host | Host library unit and contract tests, run by `make test`. |
| `design/` | host | Host library documents: `HOST.md` (layout, wire mapping, limits, configuration), `API.md`, `GRPC.md`. |
| `Makefile`, `.env.example` | host | Host library build and the environment variables it reads. |
| `bench/` | — | Benchmarks and experiments: `apps` (the DMA benchmark over the host library, `make bench`), `hpack-h2-bench`, `dmesh-router-cpp`, `dmeshgo`. |
| `docs/` | — | Working notes. |
