# Channel Comch and Go gRPC validation — 2026-09-25

Status: **PASS**. Native host/DPU builds, the complete proxy build, Go race
tests, and real gRPC payload/lifecycle tests passed in both reverse modes.
The first hardware run exposed a terminal Comch disconnect retry bug; it was
fixed and all final hardware tests used the rebuilt code. Test-owned processes
have been stopped and their listening ports released.

## Scope and changes

The public native ABI remains version 5. One process channel owns one physical
host–DPU Comch session; logical flows retain separate DMA/DPA resources.
Explicit listen and public asynchronous connect/close APIs remain later work.

Changes made for this validation:

- Go continues EQ progress while live QPs exist, including periods without a
  parked Read/Write. Initialization cleanup failures retain the channel for
  retry; listener close propagates pending connection cleanup errors.
- The native host-DPA already-armed path retains the shared control PE tick.
- Successful peer disconnect retires DPU connection aliases. A terminal
  `DOCA_ERROR_NOT_CONNECTED` result ends disconnect retries; pending sends and
  quarantined DMA resources still retain ownership.
- The standalone DPU benchmark acknowledges reader detachment after clearing
  cached pointers, so its logical CLOSE can finish.
- `cmd/channel-smoke` checks real gRPC payloads, sibling isolation, native close
  errors, flow reuse and process-channel recreation. Existing untracked user
  benchmarks were preserved.

## Testbed

| Component | Actual configuration |
|---|---|
| DPU | aarch64, `/home/youngmin/DPUMesh`, DOCA 3.5.0098 |
| Host | `youngmin@192.168.100.1` / jet1, x86_64, same project checkout |
| Host toolchain | Go 1.27.1; module requires Go 1.26 |
| DPU proxy device / representor | `03:00.1` / `0b:00.1` |
| Host Comch | `0b:00.1`, service `DPUMesh0` |
| Host DPA | `0b:00.0`, VHCA 0, EUs 0–63 |
| Proxy | One worker, CPU 15, `DMESH_BUSY_POLL=1` |
| Application registry | `/tmp/dpumesh-registry`: `10.0.1.1:8086 bench-echo 2` |
| Server / client pod addresses | `10.99.0.2` / `10.99.0.3` |
| Backend pool | Initial 2, maximum 4 |
| Control plane | Local mock identity/destination/policy on 8088/8089/8087 |

Actual data path:

```text
x86 Go gRPC client -> host DMA -> DPU Linkerd proxy
                  -> backend DMA flow -> x86 Go gRPC server
```

`DMESH_NO_TEARDOWN` was unset. The mock control plane supplied routing/identity;
the host library, Comch, DMA, DPA, proxy and Go server/client ran on real hardware.
No POSIX preload path was used for the communication tests.

## Build and software checks

| Check | Result |
|---|---|
| x86 host `make test` (includes native library/DPA kernel and ABI checks) | PASS |
| DPU `make test` | PASS |
| `ninja -C src/transport/build` | PASS |
| `ninja -C apps/dma_bench/build` | PASS |
| Full proxy release, original full LTO | PASS, 16m35s |
| Final full proxy release with disconnect fix, ThinLTO | PASS, 4m21s |
| x86 `go test -race ./...` and gofmt checks | PASS |
| Go echo examples, native echo and channel-smoke binary builds | PASS |
| Focused carrier/session-server ASan, UBSan and leak checks | PASS |
| Root and proxy submodule `git diff --check` | PASS |

The final proxy command, from `linkerd2-proxy`, was:

```sh
RUSTFLAGS='--cfg tokio_unstable -C target-cpu=native' cargo rustc \
  -p linkerd2-proxy --release --offline --bin linkerd2-proxy -- \
  -C lto=thin -C codegen-units=16 \
  -C link-arg=-L/opt/mellanox/doca/lib/aarch64-linux-gnu \
  -C link-arg=-L/opt/mellanox/flexio/lib \
  -C link-arg=-ldoca_common -C link-arg=-ldoca_dpa -C link-arg=-lflexio
```

The final link arguments satisfy the installed SDK shared-library dependencies.
Native artifacts were built separately for x86 and aarch64. `ldd` confirmed the
Go binaries loaded the x86 checkout's `build/lib/libdpumesh.so.5`. Unchanged
mock control-plane binaries were reused.

## Full-proxy hardware results

Every RPC response was compared byte-for-byte with its connection-specific
request. Payload sizes: **1, 8064, 8065, 8192, 8193 and 65537 bytes**. Sibling B
continued traffic while A repeatedly opened and closed. Each gRPC connection
had exactly one native dial; automatic reconnection could not mask disruption.
Underlying native `net.Conn.Close` errors were checked explicitly because gRPC
can otherwise discard them.

| Reverse mode | A close/reopen cycles | Verified B response bytes | B native dials | Channel close/reopen | Exit |
|---|---:|---:|---:|---|---:|
| dpu-dma | 4 | 2,157,144 | 1 | PASS | 0 |
| dpu-dma | 40 | 5,408,990 | 1 | PASS | 0 |
| host-dpa | 4 | 2,157,144 | 1 | PASS | 0 |
| host-dpa | 40 | 5,490,913 | 1 | PASS | 0 |

Both servers then exited **0** after SIGTERM, gRPC Stop, listener close and
`CloseTransport`. The same proxy remained running across both reverse modes.

Final proxy evidence:

- **1,586 outbound gRPC requests** recorded in proxy metrics.
- **10 physical Comch sessions**: two server channels and two client channel
  lifetimes per each of four client runs.
- **104 logical flows, 104 DPA pool thread releases** after server shutdown.
- **0 DOCA error log entries; 0 NOT_CONNECTED retries** in the final proxy log.
- Client channel recreation succeeded in all four runs.

Normal SDK alignment warnings and backend-unpublished notifications occurred;
no failed teardown or quarantine was reported. This validates one proxy worker
and PF devices. Multiworker routing, multiple SFs, abrupt peer failure and
sustained performance limits were not tested here.

## Reproduction

DPU launcher: `/tmp/dmesh-grpc-launch-proxy.sh`. It sources
`linkerd2-proxy/scripts/dev-proxy-env.sh`, overrides device/representor to the
values above, uses `MOCK_POLICY_ECHO_TARGET=1`, one sharded worker, busy polling,
and explicitly unsets `DMESH_NO_TEARDOWN`.

On the host, from `integrations/grpc/go`:

```sh
/usr/local/go/bin/go build -a -o bin/channel-smoke ./cmd/channel-smoke
export DPUMESH_PCI_ADDR=0b:00.1 DPUMESH_SERVER=DPUMesh0
export DPUMESH_CONFIG=/tmp/dpumesh-registry
export DPUMESH_SERVICE_IP=10.0.1.1 DPUMESH_SERVICE_PORT=8086
export DPUMESH_REVERSE=dpu-dma  # repeat with host-dpa
export DPUMESH_HOST_DPA_PCI=0b:00.0

# Server, separate process:
DPUMESH_SERVICE=bench-echo DPUMESH_POD_IP=10.99.0.2 \
DPUMESH_WORKLOAD=channel-smoke-server \
DPUMESH_BACKEND_POOL=2 DPUMESH_BACKEND_MAX=4 \
  ./bin/channel-smoke -mode server

# Client: run once with 4, then with 40 rounds.
DPUMESH_POD_IP=10.99.0.3 DPUMESH_WORKLOAD=channel-smoke-client \
  timeout 120s ./bin/channel-smoke -mode client -rounds 40 \
    -timeout 90s -rpc-timeout 5s
```

## Earlier standalone hardware check

The initial two-flow native echo passed data transfer but revealed the repeated
disconnect bug described above. After the fix, sequential two-flow 64B-message,
64B-window runs against one standalone DPU server passed in both modes:

| Reverse mode | Duration | Echoes | Exit |
|---|---:|---:|---:|
| dpu-dma | 2s | 56,609 | 0 |
| host-dpa | 2s | 51,206 | 0 |

These were transport sanity checks, not payload-integrity/performance claims.
The corrected log contains two physical sessions, four logical flows, four DPA
thread releases and no error messages. The standalone peer was stopped before
launching the full proxy.

## Artifacts and logs

Paths under `/tmp` below identify test-machine-local evidence and scripts, not
repository artifacts. The `/opt/mellanox` paths in build commands identify the
SDK installation on the test machine.

Source baseline: `03e1b9cd4fa52ba094a483da2e85545bbd904b62` plus the uncommitted
channel-Comch/Go changes. Proxy submodule baseline:
`1d9e0b0963ee445f4ff94e232d0acc8f3ba351a4` plus its working-tree changes.

| Tested artifact | SHA-256 |
|---|---|
| x86 `libdpumesh.so.5` | `727ec2ab5dc427140da2c5db9e9f1779a464d84d770f774d19febe84bdc651af` |
| x86 `channel-smoke` | `75efc10e385665238bea5bb18d05152269886bf263a98a71480f365851ce3585` |
| DPU `linkerd2-proxy` | `a25fa454bb30d25fcb1263653461beda13d756ddf6d5f03ff6339075a3764486` |
| DPU kernel archive | `4c5b84fdd524dfee19d2e3b671bbac9518320ec377b5438382d603ad5a8b5e2f` |
| Standalone `dpumesh_dpu` | `70781e311cbee500177d245f7609bc412681e2b359efc8752e2d25e4a5f14b57` |

Local evidence directory: `/tmp/dmesh-channel-grpc-20260925/`:

- `proxy.log`, `proxy-metrics.txt`, `lifecycle-counts.txt`.
- `host/dmesh-grpc-client-{dpu-dma,host-dpa}{,-stress}.log`.
- `host/dmesh-grpc-server-{dpu-dma,host-dpa}.{log,exit}`.
- `native-dpu-fixed.log`; initial failure excerpt and compressed original log.
- `host-artifacts.txt`, `dpu-artifacts.txt`.

Build logs: `/tmp/dmesh-grpc-proxy-final-build.log`,
`/tmp/dmesh-grpc-local-test.log`, `/tmp/dmesh-grpc-smoke-build.log`, and the host's
`/tmp/dmesh-grpc-host-build.log`. Tests used bounded deadlines and only their
recorded PIDs were stopped. No commit or publication was performed.
