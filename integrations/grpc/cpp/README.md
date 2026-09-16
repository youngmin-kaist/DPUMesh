# C++ gRPC

The EventEngine adapter carries standard gRPC HTTP/2 bytes over native QPs.
Generated messages, stubs, handlers and RPC deadlines stay unchanged.

```cpp
auto runtime = dpumesh::grpc::DmeshRuntime::Create(
    dpumesh::grpc::MakeNativeDmeshApiOps());
auto channel = dpumesh::grpc::CreateDmeshChannel(
    *runtime, "hello-dpumesh", grpc::InsecureChannelCredentials());
```

Check each returned status before using the value. The complete
[client](../../../bench/examples/grpc/hello_grpc_client.cc) and
[server](../../../bench/examples/grpc/hello_grpc_server.cc) show bootstrap,
error handling and an ordinary generated service. The server registers
`DPUMESH_SERVICE` and attaches its gRPC passive listener to the runtime.

Build the native library, then configure against the exact gRPC source tree:

```sh
make doca
cmake -S integrations/grpc/cpp -B build/grpc \
  -DDPUMESH_GRPC_SOURCE_DIR=/path/to/grpc-v1.80.0 \
  -DDPUMESH_GRPC_BUILD_QPS_BENCHMARK=ON -DBUILD_TESTING=ON
cmake --build build/grpc --parallel 2
ctest --test-dir build/grpc --output-on-failure
```

Run these commands from the parent repository root. CTest uses memory
transports and native doubles; device smoke executables are manual.
[Configuration](../../../README.md#configuration) applies to both processes.
See [gRPC ownership](../../../design/GRPC.md) and the separate
