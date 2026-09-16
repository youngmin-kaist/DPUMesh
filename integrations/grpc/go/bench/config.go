package bench

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// ServiceAddress identifies a virtual service, independent of the number of
// frontend streams or backend processes. The native registry owns placement.
func ServiceAddress() (string, int, error) {
	for _, key := range []string{"BENCH_K", "BENCH_W"} {
		if value := os.Getenv(key); value != "" && value != "1" {
			return "", 0, fmt.Errorf("%s no longer selects physical channels; use BENCH_P for streams and DPUMESH_SERVER per process", key)
		}
	}
	ip := os.Getenv("BENCH_IP")
	if ip == "" {
		ip = "10.0.1.1"
	}
	if parsed := net.ParseIP(ip); parsed == nil || parsed.To4() == nil {
		return "", 0, fmt.Errorf("BENCH_IP must be an IPv4 service address")
	}
	port := 8086
	if value := os.Getenv("BENCH_PORT"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 65535 {
			return "", 0, fmt.Errorf("invalid BENCH_PORT")
		}
		port = n
	}
	return ip, port, nil
}
