// Package dmesh routes hotelReservation gRPC edges over the DPUMesh DMA
// transport (DMESH_GRPC=1) or direct TCP (DMESH_TCP_DIRECT=1), bypassing the
// consul resolver in both cases. Default (neither set): stock behavior.
package dmesh

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"dmeshgo"
)

var keys = map[string]int{
	"srv-geo": 1, "srv-rate": 2, "srv-search": 3, "srv-profile": 4,
	"srv-recommendation": 5, "srv-user": 6, "srv-reservation": 7,
	"srv-review": 8, "srv-attractions": 9,
}

// TCP ports from config.json, for the direct-TCP baseline.
var ports = map[string]int{
	"srv-geo": 8083, "srv-rate": 8084, "srv-search": 8082, "srv-profile": 8081,
	"srv-recommendation": 8085, "srv-user": 8086, "srv-reservation": 8087,
	"srv-review": 8088, "srv-attractions": 8089,
}

func Enabled() bool { return os.Getenv("DMESH_GRPC") == "1" }

// Channels is the default number of per-edge gRPC connections. They share
// one process channel; backend processes each have one listener.
func Channels() int {
	if n, err := strconv.Atoi(os.Getenv("DMESH_CHANNELS")); err == nil && n > 0 {
		return n
	}
	return 1
}
func TCPDirect() bool { return os.Getenv("DMESH_TCP_DIRECT") == "1" }

// ReplicasOf parses DMESH_REPLICAS ("srv-reservation:4,srv-rate:2");
// default 1 (or the global DMESH_CHANNELS for the DMA mode).
func ReplicasOf(name string) int {
	for _, kv := range strings.Split(os.Getenv("DMESH_REPLICAS"), ",") {
		parts := strings.Split(kv, ":")
		if len(parts) == 2 && parts[0] == name {
			if n, err := strconv.Atoi(parts[1]); err == nil && n > 0 {
				return n
			}
		}
	}
	return Channels()
}

// TCPAddrs lists the direct-TCP replica addresses (port + 10000*r).
func TCPAddrs(name string) []string {
	n := ReplicasOf(name)
	out := make([]string, n)
	for r := 0; r < n; r++ {
		out[r] = fmt.Sprintf("127.0.0.1:%d", ports[name]+10000*r)
	}
	return out
}

// ServiceFromTarget extracts a known service name from a consul:// target.
func ServiceFromTarget(t string) (string, bool) {
	n := t
	if i := strings.LastIndex(t, "/"); i >= 0 {
		n = t[i+1:] // consul://addr/srv-x form
	}
	if j := strings.Index(n, "."); j > 0 {
		n = n[:j] // strip KnativeDns suffix
	}
	_, ok := keys[n]
	return n, ok
}

// Replica streams share one service key. Native placement selects a live
// backend per stream; a process opens only one physical registration.
func replicaKey(name string, _ int) (string, string) {
	return os.Getenv("DPUMESH_SERVER"), fmt.Sprintf("10.0.%d.1", 10+keys[name])
}

// Dialer returns a grpc ContextDialer for the named service over DMA. The
// addr argument names a connection-pool slot; the DPU selects its backend.
func Dialer(name string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		r := 0
		fmt.Sscanf(addr, "replica-%d", &r)
		_, ip := replicaKey(name, r)
		return dmeshgo.DialContext(ctx, ip, 8086)
	}
}

// ReplicaAddrs lists the synthetic addresses for the round-robin resolver.
func ReplicaAddrs(name string) []string {
	out := make([]string, ReplicasOf(name))
	for r := range out {
		out[r] = fmt.Sprintf("replica-%d", r)
	}
	return out
}

// TCPAddr is the direct-TCP baseline address of the named service.
func TCPAddr(name string) string {
	return fmt.Sprintf("127.0.0.1:%d", ports[name])
}

// Listen serves every native QP through one EQ-backed listener. Replicated
// backend processes register the same service with their own Pod address.
func Listen(name string, port int) (net.Listener, error) {
	if !Enabled() {
		return net.Listen("tcp", fmt.Sprintf(":%d", port))
	}
	_, ip := replicaKey(name, 0)
	return dmeshgo.ListenAddress(ip, 8086)
}
