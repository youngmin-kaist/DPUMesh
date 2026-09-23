# Go gRPC transport

`dmeshgo` implements `net.Conn` and `net.Listener` over the native API. One
process owns one channel and EQ poller; each connection owns a QP. Reads retain
native RX leases until consumed, and writes resume on EQ readiness. Concurrent
read/write, deadlines and connection close follow the Go networking contract.

Build the native library from the repository root, then compile the module:

```sh
make lib
(cd integrations/grpc/go && go test ./...)
```

Configure `DPUMESH_PCI_ADDR`, `DPUMESH_POD_IP`, `DPUMESH_SERVER` and the shared
`DPUMESH_CONFIG` registry before opening a connection. A server additionally
sets `DPUMESH_SERVICE`. [Root configuration](../../../README.md#configuration)
defines these values. The older `Dial`/`Listen` signatures accept only labels
that agree with this process configuration; they do not create separate
physical registrations.

Use `DialContext(ctx, serviceIP, port)` in `grpc.WithContextDialer` and
`ListenAddress(serviceIP, port)` with `grpc.Server.Serve`. A service address
identifies a registry entry; the DPU chooses its native backend. The examples
in `cmd/echo-client` and `cmd/echo-server` run the standard gRPC health RPC
against `DPUMESH_SERVICE_IP:DPUMESH_SERVICE_PORT`.
Close all connections and listeners before calling `CloseTransport`.
