// gRPC echo (health) server listening ON THE DMA CHANNEL: it registers itself
// with the DPU proxy as the backend for the echo service key, and serves h2
// connections the proxy opens through the channel.
package main

import (
	"os"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dmeshgo"
)

func main() {
	srv := os.Getenv("DMESH_SERVER")
	if srv == "" {
		srv = "DPUMesh0"
	}
	lis, err := dmeshgo.Listen(srv, "10.0.0.42", 8086, "echo-server.dmesh")
	if err != nil {
		log.Fatalf("dmesh listen: %v", err)
	}
	log.Printf("echo-server: backend channel registered for 10.0.0.42:8086 (server DPUMesh0)")

	s := grpc.NewServer(grpc.ConnectionTimeout(24 * time.Hour))
	h := health.NewServer()
	h.SetServingStatus("echo", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, h)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
