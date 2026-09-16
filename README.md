# DPUMesh

| Directory | Side | Contents |
|---|---|---|
| `DPUMesh/` | DPU | The C DOCA data plane (`dpumesh`) and its DPA kernel, a Meson project. Its host-side sources are also compiled into the proxy and into the host library. |
| `linkerd2-proxy/` | DPU | The linkerd2-proxy fork with the DMA transport (git submodule). Its build compiles `../../../DPUMesh/*.c`, so the two directories stay side by side. |
| `include/`, `src/` | host | The DPUmesh host library: public API (`include/dpumesh`), core and carrier (`src/core`), native API and POSIX preload façades (`src/facade`). |
| `integrations/grpc/` | host | C++ and Go gRPC adapters over the host library. |
| `examples/` | host | One working example per API: native, preload, gRPC. |
| `tests/` | host | Host library unit and contract tests, run by `make test`. |
| `design/` | host | Host library documents: `HOST.md` (layout, wire mapping, limits, configuration), `API.md`, `GRPC.md`. |
| `Makefile`, `.env.example` | host | Host library build and the environment variables it reads. |
| `bench/` | — | Benchmarks and experiments: `hpack-h2-bench`, `dmesh-router-cpp`, `dmeshgo`. |
| `docs/` | — | Working notes. |
