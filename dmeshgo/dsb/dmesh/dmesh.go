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

func Enabled() bool   { return os.Getenv("DMESH_GRPC") == "1" }

// Channels is the per-edge DMA channel count (the replica analogue: each
// service registers this many backend listeners and every client spreads
// RPCs round-robin over this many connections).
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

// clientBase offsets the synthetic src port per client process
// (DMESH_CLIENT_ID) so replicated frontends do not present identical 4-tuples.
func clientBase() int {
	if n, err := strconv.Atoi(os.Getenv("DMESH_CLIENT_ID")); err == nil && n > 0 {
		return 42000 + 1000*n
	}
	return 42000
}

func workers() int {
	if n, err := strconv.Atoi(os.Getenv("DMESH_W")); err == nil && n > 0 {
		return n
	}
	return 8
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

// replicaKey maps (service, replica) to its DPU worker and dst key. The worker
// is chosen by a global channel index (services in idx order, each expanded by
// its replica count, from the shared DMESH_REPLICAS env) so backends spread
// evenly over W workers instead of neighbouring services piling onto the same
// worker (8 DPA slots each). Client and server compute the same mapping.
func replicaKey(name string, r int) (string, string) {
	idx := keys[name]
	gid := 0
	for _, s := range keyOrder {
		if keys[s] >= idx {
			break
		}
		gid += ReplicasOf(s)
	}
	gid += r
	srv := fmt.Sprintf("DPUMesh%d", gid%workers())
	ip := fmt.Sprintf("10.0.%d.%d", 10+idx, 1+r)
	return srv, ip
}

var keyOrder = []string{"srv-geo", "srv-rate", "srv-search", "srv-profile",
	"srv-recommendation", "srv-user", "srv-reservation", "srv-review", "srv-attractions"}

// Dialer returns a grpc ContextDialer for the named service over DMA. The
// addr argument ("replica-<r>") selects the target channel replica.
func Dialer(name string) func(context.Context, string) (net.Conn, error) {
	idx := keys[name]
	return func(ctx context.Context, addr string) (net.Conn, error) {
		r := 0
		fmt.Sscanf(addr, "replica-%d", &r)
		srv, ip := replicaKey(name, r)
		return dmeshgo.Dial(srv, "127.0.0.1", clientBase()+idx*8+r, ip, 8086, "hotelres-client")
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

// combinedListener fans in Accepts from N per-replica DMA listeners.
type combinedListener struct {
	subs  []net.Listener
	conns chan acceptResult
}

type acceptResult struct {
	c   net.Conn
	err error
}

func (l *combinedListener) Accept() (net.Conn, error) {
	r := <-l.conns
	return r.c, r.err
}
func (l *combinedListener) Close() error {
	for _, s := range l.subs {
		s.Close()
	}
	return nil
}
func (l *combinedListener) Addr() net.Addr { return l.subs[0].Addr() }

// Listen returns the service listener: N DMA backend listeners (one per
// channel replica) when DMESH_GRPC=1, else plain TCP on the configured port.
func Listen(name string, port int) (net.Listener, error) {
	if !Enabled() {
		return net.Listen("tcp", fmt.Sprintf(":%d", port))
	}
	if ri := os.Getenv("DMESH_REPLICA_IDX"); ri != "" {
		r, _ := strconv.Atoi(ri)
		srv, ip := replicaKey(name, r)
		return dmeshgo.Listen(srv, ip, 8086, name)
	}
	n := ReplicasOf(name)
	cl := &combinedListener{conns: make(chan acceptResult, n)}
	for r := 0; r < n; r++ {
		srv, ip := replicaKey(name, r)
		sub, err := dmeshgo.Listen(srv, ip, 8086, name)
		if err != nil {
			return nil, err
		}
		cl.subs = append(cl.subs, sub)
		go func(sub net.Listener) {
			for {
				c, err := sub.Accept()
				cl.conns <- acceptResult{c, err}
				if err != nil {
					return
				}
			}
		}(sub)
	}
	return cl, nil
}
