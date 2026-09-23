// Echo bench server: registers K backend listeners (dst keys 10.0.1.(1+i))
// with the DPU proxy over DMA channels and serves the raw echo RPC on each.
// Env: BENCH_K (listeners), BENCH_W (DPU workers, listener i -> DPUMesh(i%W)).
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"dmeshgo"
	"dmeshgo/bench"
)

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return d
}

func main() {
	k := envInt("BENCH_K", 1)
	w := envInt("BENCH_W", 1)

	s := grpc.NewServer(
		grpc.ForceServerCodec(bench.RawCodec{}),
		grpc.ConnectionTimeout(24*time.Hour),
	)
	bench.RegisterEcho(s)

	for i := 0; i < k; i++ {
		srv := fmt.Sprintf("DPUMesh%d", i%w)
		ip := fmt.Sprintf("10.0.1.%d", 1+i)
		lis, err := dmeshgo.Listen(srv, ip, 8086, "bench-server.dmesh")
		if err != nil {
			log.Fatalf("listen %d (%s via %s): %v", i, ip, srv, err)
		}
		go func(i int) {
			if err := s.Serve(lis); err != nil {
				log.Printf("serve %d exited: %v", i, err)
			}
		}(i)
		log.Printf("bench-server: listener %d registered (%s:8086 via %s)", i, ip, srv)
	}
	select {}
}
