// gRPC echo (health) client dialing THROUGH THE DPU PROXY over DMA: the
// custom dialer returns a dmesh channel instead of a TCP conn; the proxy
// terminates h2, routes by the flow's dst key, and reaches the echo server's
// backend channel.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dmeshgo"
)

func main() {

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		log.Printf("echo-client: dialing DMA channel (dst key %s:%d)", serviceIP(), servicePort())
		_ = log.Printf
		log.Printf("")
		return dmeshgo.DialContext(ctx, serviceIP(), servicePort())
	}

	cc, err := grpc.NewClient("passthrough:///dmesh-echo",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc client: %v", err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{Service: "echo"})
	if err != nil {
		log.Fatalf("echo RPC failed: %v", err)
	}
	log.Printf("ECHO OK: status=%s rtt=%v (cold: includes DMA channel setup)",
		resp.GetStatus(), time.Since(start))

	// Warm RTT: the channel and h2 connections are up now.
	start = time.Now()
	resp, err = healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{Service: "echo"})
	if err != nil {
		log.Fatalf("second echo RPC failed: %v", err)
	}
	log.Printf("ECHO OK: status=%s rtt=%v (warm: client → DMA → DPU proxy L7 → DMA → server)",
		resp.GetStatus(), time.Since(start))
}

// The service address is the registry row the DPU routes; override for a bench registry.
func serviceIP() string {
	if v := os.Getenv("DMESH_SERVICE_IP"); v != "" {
		return v
	}
	return "10.0.0.42"
}
func servicePort() int {
	if v := os.Getenv("DMESH_SERVICE_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8086
}
