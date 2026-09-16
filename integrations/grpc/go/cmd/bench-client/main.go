// P gRPC connections share a native channel and address one virtual service.
// BENCH_P/M control connections/concurrency; BENCH_IP/PORT identify the service.
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
	ip, port, err := bench.ServiceAddress()
	if err != nil {
		log.Fatal(err)
	}
	payload := envInt("BENCH_PAYLOAD", 64)
	warm := time.Duration(envInt("BENCH_WARM", 3)) * time.Second
	dur := time.Duration(envInt("BENCH_DUR", 10)) * time.Second

	req := bytes.Repeat([]byte{0xAB}, payload)

	// P QPs share one native registration.
	defer dmeshgo.CloseTransport()
	conns := make([]*grpc.ClientConn, p)
	defer func() {
		for _, conn := range conns {
			if conn != nil {
				conn.Close()
			}
		}
	}()
	for i := 0; i < p; i++ {
		dialer := func(ctx context.Context, addr string) (net.Conn, error) {
			return dmeshgo.DialContext(ctx, ip, port)
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
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	var total int64
	var failed atomic.Bool
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
				defer func() { atomic.AddInt64(&total, local) }()
				ctx := runCtx
				for !stop.Load() {
					if err := cc.Invoke(ctx, bench.MethodPing, req, &out, grpc.ForceCodec(bench.RawCodec{})); err != nil {
						if !stop.Load() {
							failed.Store(true)
							log.Printf("rpc error: %v", err)
						}
						return
					}
					if !bytes.Equal(out, req) {
						failed.Store(true)
						return
					}
					if measuring.Load() {
						local++
					}
				}
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
	cancelRun()
	wg.Wait()
	if failed.Load() {
		log.Fatal("benchmark failed: RPC error or payload mismatch")
	}

	log.Printf("RESULT: %.0f req/s (P=%d M=%d payload=%dB dur=%v total=%d)",
		float64(total)/elapsed.Seconds(), p, m, payload, elapsed.Round(time.Millisecond), total)
}
