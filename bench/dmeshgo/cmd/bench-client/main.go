// Echo bench client: P gRPC connections over DMA channels through the DPU
// proxy (conn i -> dst key 10.0.1.(1+i%K) via DPUMesh(i%W)), M concurrent 64B
// Ping RPC loops per connection. Prints an aggregate req/s line.
//
// Env: BENCH_P (conns), BENCH_M (in-flight per conn), BENCH_K, BENCH_W,
// BENCH_PAYLOAD (bytes, default 64), BENCH_WARM/BENCH_DUR (seconds).
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	p := envInt("BENCH_P", 1)
	m := envInt("BENCH_M", 64)
	k := envInt("BENCH_K", p)
	w := envInt("BENCH_W", 1)
	payload := envInt("BENCH_PAYLOAD", 64)
	warm := time.Duration(envInt("BENCH_WARM", 3)) * time.Second
	dur := time.Duration(envInt("BENCH_DUR", 10)) * time.Second

	req := bytes.Repeat([]byte{0xAB}, payload)

	// P connections, each over its own DMA channel.
	conns := make([]*grpc.ClientConn, p)
	for i := 0; i < p; i++ {
		i := i
		srv := fmt.Sprintf("DPUMesh%d", i%w)
		ip := fmt.Sprintf("10.0.1.%d", 1+i%k)
		dialer := func(ctx context.Context, addr string) (net.Conn, error) {
			return dmeshgo.Dial(srv, "127.0.0.1", 41000+i, ip, 8086, "bench-client.dmesh")
		}
		cc, err := grpc.NewClient(fmt.Sprintf("passthrough:///bench-%d", i),
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("client %d: %v", i, err)
		}
		conns[i] = cc
	}

	// Preflight: one verified echo per connection BEFORE any load.
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i, cc := range conns {
		var out []byte
		if err := cc.Invoke(pctx, bench.MethodPing, req, &out, grpc.ForceCodec(bench.RawCodec{})); err != nil {
			log.Fatalf("preflight conn %d failed: %v", i, err)
		}
		if !bytes.Equal(out, req) {
			log.Fatalf("preflight conn %d: echo mismatch (%d bytes back)", i, len(out))
		}
	}
	pcancel()
	log.Printf("preflight OK: %d conns, %dB echo verified", p, payload)

	// Warmup + timed measurement.
	var total int64
	var stop atomic.Bool
	var measuring atomic.Bool
	var wg sync.WaitGroup
	for _, cc := range conns {
		for j := 0; j < m; j++ {
			wg.Add(1)
			go func(cc *grpc.ClientConn) {
				defer wg.Done()
				var out []byte
				var local int64
				ctx := context.Background()
				for !stop.Load() {
					if err := cc.Invoke(ctx, bench.MethodPing, req, &out, grpc.ForceCodec(bench.RawCodec{})); err != nil {
						if !stop.Load() {
							log.Printf("rpc error: %v", err)
						}
						return
					}
					if measuring.Load() {
						local++
					}
				}
				atomic.AddInt64(&total, local)
			}(cc)
		}
	}
	time.Sleep(warm)
	measuring.Store(true)
	start := time.Now()
	time.Sleep(dur)
	measuring.Store(false)
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()

	log.Printf("RESULT: %.0f req/s (P=%d M=%d payload=%dB dur=%v total=%d)",
		float64(total)/elapsed.Seconds(), p, m, payload, elapsed.Round(time.Millisecond), total)
}
