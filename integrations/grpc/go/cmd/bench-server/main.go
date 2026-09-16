// Serve raw echo RPCs through one native service registration. Scale backend
// replicas with separate processes sharing DPUMESH_SERVICE and BENCH_IP.
package main

import (
	"dmeshgo"
	"dmeshgo/bench"
	"google.golang.org/grpc"
	"log"
	"time"
)

func main() {
	ip, port, err := bench.ServiceAddress()
	if err != nil {
		log.Fatal(err)
	}
	lis, err := dmeshgo.ListenAddress(ip, port)
	if err != nil {
		log.Fatal(err)
	}
	defer dmeshgo.CloseTransport()
	defer lis.Close()
	server := grpc.NewServer(grpc.ForceServerCodec(bench.RawCodec{}),
		grpc.ConnectionTimeout(24*time.Hour))
	bench.RegisterEcho(server)
	log.Printf("bench-server: native service at %s:%d", ip, port)
	if err := server.Serve(lis); err != nil {
		log.Printf("serve: %v", err)
	}
}
