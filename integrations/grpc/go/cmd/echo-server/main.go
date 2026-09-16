// gRPC echo (health) server listening ON THE DMA CHANNEL: it registers itself
// with the DPU proxy as the backend for the echo service key, and serves h2
// connections the proxy opens through the channel.
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dmeshgo"
)

func main() {

	lis, err := dmeshgo.ListenAddress(serviceIP(), servicePort())
	if err != nil {
		log.Fatalf("dmesh listen: %v", err)
	}
	log.Printf("echo-server: backend channel registered for %s:%d (configured DPUMESH_SERVER)", serviceIP(), servicePort())

	s := grpc.NewServer(grpc.ConnectionTimeout(24 * time.Hour))
	h := health.NewServer()
	h.SetServingStatus("echo", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, h)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
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
