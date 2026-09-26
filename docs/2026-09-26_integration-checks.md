# Channel Comch integration checks — 2026-09-26

The current native transport, proxy library and Go sources passed the software
checks below before integration into the parent repository's `main` branch.
These checks did not open DOCA devices or change SFs, EU partitions, network
settings or firmware. They are separate from the earlier hardware experiments.

## Revisions and scope

- Native transport: `05e7ebb`.
- Go adapter and lifecycle smoke fixture: `b252816`.
- Verified channel benchmark: `ec72182`.
- Preserved standalone benchmark tools: `8def20a`.
- Proxy submodule: `90915213` (reader-detachment fence).

The host tests used a snapshot of these working-tree source files in an
isolated directory; the existing host checkout was not overwritten. The Go
module linked the native library built from that same snapshot.

The public C ABI remains 5. The private Comch session protocol and DPA argument
layout require matching host, DPU and proxy revisions. DPA runtime sharing,
the host broker, per-channel DPA thread/ring changes, and public asynchronous
connect/listen/close APIs remain design work, not features of these commits.

## Checks

| Check | Environment | Result |
|---|---|---|
| `make -j4 test` | DPU aarch64, DOCA 3.5 | PASS: native/mock tests, header and ABI checks |
| `ninja -C src/transport/build` | DPU aarch64 | PASS |
| `cargo test -p dmesh-doca --offline` with SDK link flags | DPU aarch64 | PASS: 12 library tests, 0 failures |
| `make -j4 test` | Isolated x86 host snapshot, DOCA 3.5 | PASS |
| `go test -race ./...` | Same host snapshot, Go 1.27.1 | PASS, including channel-bench and channel-smoke unit tests |
| `gofmt -l dmesh.go dmesh_test.go bench cmd` | Same Go source snapshot | No formatting changes needed |
| `git diff --check` | Parent and proxy | PASS before commits |

Proxy test invocation, from the submodule:

```sh
RUSTFLAGS='-C link-arg=-L/opt/mellanox/doca/lib/aarch64-linux-gnu -C link-arg=-L/opt/mellanox/flexio/lib -C link-arg=-ldoca_common -C link-arg=-ldoca_dpa -C link-arg=-lflexio' \
  cargo test -p dmesh-doca --offline
```

Local logs were stored under `/tmp/dmesh-functional-commits-20260926/` on the
host and `/tmp/dmesh-functional-commits-*.log` on the DPU. These are temporary
test-machine paths, not repository artifacts.

## Hardware evidence and remaining limits

The full proxy build and actual DMA/gRPC lifecycle checks are recorded in the
[2026-09-25 hardware report](2026-09-25_channel-comch-grpc-validation.md).
This integration check did not rerun the full hardware benchmark suite.
The [experiment index](../bench-results/README.md) retains subsequent failures
and incomplete measurements, including host-dpa scaling teardown `EBADMSG`,
the 28-base-process boundary and the independent EU budget.

Source review also identified a pre-existing slot-reuse race in the Rust
registration boundary. `ConnReady` and `Registration` carry a slot but no slot
generation. If A's event/registration is delayed, A closes, and B reuses the
same slot before A registers, the current Running-state check cannot identify
the old registration. This condition existed at proxy baseline `1d9e0b09` and
parent baseline `03e1b9c`; it was not introduced by the reader-fence change.
It has not been reproduced on hardware in this integration check. A follow-up
should carry the slot incarnation through event, registration and staging
binding, with a deterministic delayed-registration regression test. C wire
generation checks do not cover this Rust registration boundary.

Process-wide DPU pool/base cleanup is also incomplete; the current void driver
drop path retains resources when it cannot prove safe cleanup. Retention is
not a claim of complete resource reclamation. The
[DPA sharing plan](2026-09-26_dpa-process-sharing-plan.md) includes checked
runtime ownership and the worker shutdown barrier as future work.
