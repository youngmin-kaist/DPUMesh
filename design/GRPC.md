# gRPC integration

`integrations/grpc/cpp` contains the source DPUmesh adapter for gRPC 1.80.0. Its
EventEngine endpoint implements byte-stream IO beneath chttp2. HTTP/2 framing,
RPC deadlines and RPC semantics remain in gRPC. The adapter uses the public
native API through `dmesh_api_ops.cc`.

## Protobuf boundary

The EventEngine byte path receives `SliceBuffer` writes after stock gRPC has serialized
messages and framed HTTP/2. `DmeshEndpoint::PumpWrite` batches consecutive
slices into native reservations; `DmeshReactor::Post` reserves, fills and
commits those bytes. This path offloads transport and preserves ordinary
generated stubs. It does not submit protobuf objects to a DPA serializer.

`doca/grpc/grpc_wire_encode.*` provides a separate schema-based encoder.The encoder produces a five-byte gRPC
message prefix and protobuf payload; HTTP/2 framing remains a separate layer.
Its reverse variant returns the output's starting offset as well as a
completion length. These values must both be preserved by the caller.

The flat-object prefix describes the input allocation, not a protobuf
payload. A byte-stream endpoint cannot identify such an object from ordinary
gRPC bytes.A DPA dispatcher and
four workers serialize them before forwarding, preserving output custody and
reverse offsets. This raw message path is separate from the standard C++/Go
gRPC HTTP/2 endpoint and is not accepted by embedded Linkerd. Local parity and
scheduler tests do not establish hardware performance.

The C++ RPC adapter below instead returns serialized output to the calling
Host, which supplies it to gRPC before message and HTTP/2 framing. These local
accelerator requests are also accepted alongside embedded Linkerd.

## Runtime and ownership

`DmeshRuntime` uses one channel and a configurable set of EQ reactor shards.
Outbound connections are assigned across shards; accepted QPs belong to the
reactor receiving the connection event. Each reactor owns EQ polling, command
processing and normal receive/TX-ready progress. A shared executor delivers
deferred callbacks. Endpoint destruction and transport failure complete
pending operations once.

Reads copy native fragments into gRPC slices. The reactor can retain RX credit
when the endpoint is above its high-water mark; retention is bounded per
connection and excessive accumulation fails that connection. Writes reserve
native memory, commit bytes and resume on TX-ready after backpressure. The
adapter does not own DOCA registration or a broker process.

## Build and verification

```sh
cmake -S integrations/grpc/cpp -B build/grpc \
  -DDPUMESH_GRPC_SOURCE_DIR=/path/to/grpc-v1.80.0 \
  -DDPUMESH_GRPC_BUILD_QPS_BENCHMARK=OFF -DBUILD_TESTING=ON
cmake --build build/grpc --parallel 2
ctest --test-dir build/grpc --output-on-failure
```

The source checkout needs the gRPC CMake dependencies. An exact installed
gRPC package is also accepted when the source-directory option is omitted.
Endpoint, channel and reactor tests use in-memory transports/native doubles.
They exercise byte integrity, callback ownership, cancellation, readiness,
credit bounds and lifecycle without DOCA initialization.

The native aggregate, native link/smoke programs and gRPC benchmark applications
are enabled only when the public headers and a native library are available.
Library discovery is restricted to the selected parent tree, with explicit
CMake overrides, so an installed library from another implementation cannot
silently satisfy this dependency. `make doca` produces the native
library and its local discovery links. Device smoke programs are not registered as automatic CTest tests.
