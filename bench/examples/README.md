# Application examples

The examples address a service through the native transport. Host processes
need access to the DOCA device and a running DPU Comch server. Both sides use
the same static registry; see [configuration](../../README.md#configuration).

## Native C

`hello_dpumesh.c` and `hello_dpumesh_server.c` show channel/EQ/QP creation,
reservation and commit, receive-buffer release, readiness and teardown.

```sh
make examples
DPUMESH_SERVICE=hello-dpumesh build/bin/hello_dpumesh_server
# In another configured Host process:
build/bin/hello_dpumesh hello-dpumesh hello
```

The registry must contain `hello-dpumesh`. Each process supplies its own
`DPUMESH_POD_IP`; client-only processes leave `DPUMESH_SERVICE` unset.

## C++ gRPC

The [client](grpc/hello_grpc_client.cc) creates a DPUmesh runtime and channel;
the [server](grpc/hello_grpc_server.cc) attaches its ordinary generated service.
The `.proto`, generated messages and RPC handler remain stock gRPC.
Build them with the C++ adapter's `DPUMESH_GRPC_BUILD_EXAMPLES=ON` option (default);
see [gRPC](../../design/GRPC.md).
