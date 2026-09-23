// Package dmeshgo adapts the native channel/EQ/QP API to net.Conn and
// net.Listener. One process owns one channel and one polling goroutine.
// Configure DPUMESH_PCI_ADDR, DPUMESH_SERVER, DPUMESH_POD_IP,
// DPUMESH_CONFIG and, for a listener, DPUMESH_SERVICE before starting Go.
package dmeshgo

/*
#cgo CFLAGS: -D_GNU_SOURCE -I${SRCDIR}/../../../include
#cgo LDFLAGS: -L${SRCDIR}/../../../build/lib -ldpumesh -Wl,-rpath,${SRCDIR}/../../../build/lib
#include <stdlib.h>
#include <string.h>
#include "dpumesh/dmesh.h"
#include <poll.h>
#include <time.h>
#include <unistd.h>
static void dmesh_go_wait_fd(int fd, int64_t timeout_ns) {
    if (timeout_ns < 0 || timeout_ns > 1000000) timeout_ns = 1000000;
    struct timespec timeout = {0, timeout_ns};
    struct pollfd event = {.fd = fd, .events = POLLIN};
    if (ppoll(&event, 1, &timeout, NULL) > 0 && (event.revents & POLLIN)) {
        uint64_t count;
        while (read(fd, &count, sizeof(count)) == sizeof(count)) {}
    }
}
*/
import "C"

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	ModeBackend     = 1
	ModeIngressPush = 2
)

// PCIAddr must agree with the native process configuration.
var PCIAddr = envOr("DPUMESH_PCI_ADDR", "")

func envOr(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "dmesh: i/o deadline exceeded" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Unwrap() error   { return os.ErrDeadlineExceeded }

// All native calls and EQ event dispatch share this mutex. Read and Write
// hold their own direction locks across waits, as required by net.Conn.
// RX events retain native leases; they are copied directly into the caller's
// slice and released only after the last byte has been read.
type transport struct {
	mu       sync.Mutex
	ch       *C.dmesh_channel_t
	eq       *C.dmesh_eq_t
	conns    map[*C.dmesh_qp_t]*Conn
	listener *Listener
	changed  chan struct{}
	stop     chan struct{}
	done     chan struct{}
	err      error
}

var process struct {
	sync.Mutex
	t *transport
}

func (t *transport) notify() { close(t.changed); t.changed = make(chan struct{}) }
func openTransport() (*transport, error) {
	process.Lock()
	defer process.Unlock()
	if process.t != nil {
		return process.t, nil
	}
	ch, err := C.dmesh_create_channel()
	if ch == nil {
		return nil, fmt.Errorf("dmesh: create channel: %w", err)
	}
	eq, err := C.dmesh_create_eq(ch)
	if eq == nil {
		C.dmesh_destroy_channel(ch)
		return nil, fmt.Errorf("dmesh: create EQ: %w", err)
	}
	t := &transport{ch: ch, eq: eq, conns: make(map[*C.dmesh_qp_t]*Conn),
		changed: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	process.t = t
	go t.poll()
	return t, nil
}

func (t *transport) poll() {
	defer close(t.done)
	fd := C.dmesh_eq_fd(t.eq)
	var events [64]C.dmesh_event_t
	for {
		select {
		case <-t.stop:
			return
		default:
		}
		t.mu.Lock()
		count, err := C.dmesh_poll_eq(t.eq, &events[0], C.int(len(events)))
		if count < 0 {
			t.err = err
			t.notify()
			t.mu.Unlock()
			return
		}
		// A QP pointer can occur more than once in this batch. Destruction is
		// deferred until every event has been dispatched.
		var reject map[*C.dmesh_qp_t]bool
		for i := 0; i < int(count); i++ {
			ev := events[i]
			c := t.conns[ev.qp]
			if ev._type == C.DMESH_EVENT_CONN_REQ && c == nil {
				if t.listener == nil || t.listener.closed {
					if reject == nil {
						reject = make(map[*C.dmesh_qp_t]bool)
					}
					reject[ev.qp] = true
					continue
				}
				c = newConn(t, ev.qp, t.listener.addr, &net.TCPAddr{Port: int(ev.qp.remote_port)})
				t.conns[ev.qp] = c
				t.listener.pending = append(t.listener.pending, c)
			}
			if c == nil {
				C.dmesh_release_rx_buffer(t.ch, &ev)
				continue
			}
			switch ev._type {
			case C.DMESH_EVENT_RECV:
				c.rx = append(c.rx, ev)
			case C.DMESH_EVENT_RECV_FIN:
				c.eof = true
			case C.DMESH_EVENT_TX_ERROR:
				c.err = syscall.EIO
			}
		}
		for qp := range reject {
			C.dmesh_abort_qp(qp)
		}
		if count != 0 {
			t.notify()
		}
		deadline := C.dmesh_eq_next_deadline_ns(t.eq)
		t.mu.Unlock()
		if count == C.int(len(events)) {
			continue
		}
		// EQ readiness wakes immediately; the bounded timeout also observes stop
		// and supports poll-only EQs when eventfd creation was unavailable.
		C.dmesh_go_wait_fd(fd, deadline)
	}
}

// CloseTransport closes the idle process channel. Call after closing all
// listeners and connections; live objects cause EBUSY without invalidation.
func CloseTransport() error {
	process.Lock()
	defer process.Unlock()
	t := process.t
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if len(t.conns) != 0 || t.listener != nil {
		t.mu.Unlock()
		return syscall.EBUSY
	}
	// An opener can already hold t after dropping process.Mutex. Mark the
	// transport closed under the same lock that guards QP/listener creation
	// before stopping the poller or destroying its EQ. A failed cleanup keeps
	// this state until CloseTransport can retry successfully.
	t.err = net.ErrClosed
	t.notify()
	t.mu.Unlock()
	select {
	case <-t.stop:
	default:
		close(t.stop)
	}
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.eq != nil {
		if n, err := C.dmesh_destroy_eq(t.eq); n != 0 {
			return err
		}
		t.eq = nil
	}
	if n, err := C.dmesh_destroy_channel(t.ch); n != 0 {
		return err
	}
	t.ch = nil
	process.t = nil
	return nil
}

type Conn struct {
	t               *transport
	qp              *C.dmesh_qp_t
	local, remote   net.Addr
	readMu, writeMu sync.Mutex
	rx              []C.dmesh_event_t
	pos             int
	rd, wd          time.Time
	closed, eof     bool
	err             error
}

func newConn(t *transport, qp *C.dmesh_qp_t, local, remote net.Addr) *Conn {
	return &Conn{t: t, qp: qp, local: local, remote: remote}
}
func waitChange(ch <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		<-ch
		return nil
	}
	left := time.Until(deadline)
	if left <= 0 {
		return timeoutError{}
	}
	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return timeoutError{}
	}
}
func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	t := c.t
	for {
		t.mu.Lock()
		if c.closed {
			t.mu.Unlock()
			return 0, net.ErrClosed
		}
		if len(p) == 0 {
			t.mu.Unlock()
			return 0, nil
		}
		if !c.rd.IsZero() && !time.Now().Before(c.rd) {
			t.mu.Unlock()
			return 0, timeoutError{}
		}
		if len(c.rx) != 0 {
			ev := &c.rx[0]
			src := unsafe.Slice((*byte)(unsafe.Pointer(ev.buf)), int(ev.len))
			n := copy(p, src[c.pos:])
			c.pos += n
			if c.pos == len(src) {
				C.dmesh_release_rx_buffer(t.ch, ev)
				c.rx[0] = C.dmesh_event_t{}
				c.rx = c.rx[1:]
				c.pos = 0
			}
			t.mu.Unlock()
			return n, nil
		}
		err := c.err
		if err == nil {
			err = t.err
		}
		if err == nil && c.eof {
			err = io.EOF
		}
		if err != nil {
			t.mu.Unlock()
			return 0, err
		}
		ch, deadline := t.changed, c.rd
		t.mu.Unlock()
		// Re-read state under the lock: a deadline extension can race the
		// previous timer firing, and close/error takes precedence on wake.
		_ = waitChange(ch, deadline)
	}
}
func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	t := c.t
	written := 0
	for written < len(p) {
		t.mu.Lock()
		err := c.err
		if err == nil {
			err = t.err
		}
		if c.closed {
			err = net.ErrClosed
		}
		if err == nil && !c.wd.IsZero() && !time.Now().Before(c.wd) {
			err = timeoutError{}
		}
		if err != nil {
			t.mu.Unlock()
			return written, err
		}
		n := len(p) - written
		if max := int(C.dmesh_post_max(t.ch)); n > max {
			n = max
		}
		dst, err := C.dmesh_alloc(c.qp, C.uint32_t(n))
		if dst == nil {
			if !errors.Is(err, syscall.EAGAIN) {
				t.mu.Unlock()
				return written, err
			}
			ch, deadline := t.changed, c.wd
			t.mu.Unlock()
			_ = waitChange(ch, deadline)
			continue
		}
		C.memcpy(dst, unsafe.Pointer(&p[written]), C.size_t(n))
		rc, err := C.dmesh_post_send(c.qp, dst, C.uint32_t(n))
		if rc != 0 {
			t.mu.Unlock()
			return written, err
		}
		written += n
		t.mu.Unlock()
	}
	return written, nil
}
func (c *Conn) closeLocked(abort bool) error {
	if c.closed {
		return nil
	}
	c.closed = true
	for i := range c.rx {
		C.dmesh_release_rx_buffer(c.t.ch, &c.rx[i])
	}
	c.rx = nil
	delete(c.t.conns, c.qp)
	var rc C.int
	var err error
	if abort {
		rc, err = C.dmesh_abort_qp(c.qp)
	} else {
		rc, err = C.dmesh_destroy_qp(c.qp)
	}
	c.qp = nil
	c.t.notify()
	if rc != 0 {
		return err
	}
	return nil
}
func (c *Conn) Close() error         { c.t.mu.Lock(); defer c.t.mu.Unlock(); return c.closeLocked(false) }
func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }
func (c *Conn) SetDeadline(d time.Time) error {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.rd, c.wd = d, d
	c.t.notify()
	return nil
}
func (c *Conn) SetReadDeadline(d time.Time) error {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.rd = d
	c.t.notify()
	return nil
}
func (c *Conn) SetWriteDeadline(d time.Time) error {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.wd = d
	c.t.notify()
	return nil
}

func serviceAt(ip string, port int) (string, error) {
	path := envOr("DPUMESH_CONFIG", "/etc/dpumesh/registry")
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	addr := net.JoinHostPort(ip, fmt.Sprint(port))
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(strings.SplitN(scan.Text(), "#", 2)[0])
		if len(fields) == 3 && fields[0] == addr {
			return fields[1], nil
		}
	}
	if err := scan.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("dmesh: %s is absent from %s", addr, path)
}
func checkConfig(server, pod, workload string) error {
	for _, value := range [][3]string{{"DPUMESH_SERVER", server, "DPUMesh0"}, {"DPUMESH_POD_IP", pod, ""},
		{"DPUMESH_WORKLOAD", workload, ""}, {"DPUMESH_PCI_ADDR", PCIAddr, ""}} {
		if value[1] != "" && value[1] != envOr(value[0], value[2]) {
			return fmt.Errorf("dmesh: argument disagrees with process %s; configure it before opening the channel", value[0])
		}
	}
	return nil
}
func Dial(server, srcIP string, srcPort int, dstIP string, dstPort int, workload string) (net.Conn, error) {
	if err := checkConfig(server, srcIP, workload); err != nil {
		return nil, err
	}
	service, err := serviceAt(dstIP, dstPort)
	if err != nil {
		return nil, err
	}
	t, err := openTransport()
	if err != nil {
		return nil, err
	}
	name := C.CString(service)
	defer C.free(unsafe.Pointer(name))
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return nil, t.err
	}
	qp, err := C.dmesh_create_qp(t.eq, name)
	if qp == nil {
		return nil, err
	}
	// Native port allocation owns the stream identifier; srcPort is a caller
	// label only and never overrides the QP allocator.
	c := newConn(t, qp, &net.TCPAddr{IP: net.ParseIP(envOr("DPUMESH_POD_IP", srcIP)), Port: int(qp.local_port)},
		&net.TCPAddr{IP: net.ParseIP(dstIP), Port: dstPort})
	t.conns[qp] = c
	return c, nil
}

type Listener struct {
	t       *transport
	addr    net.Addr
	pending []*Conn
	closed  bool
}

func Listen(server, svcIP string, svcPort int, workload string) (*Listener, error) {
	if err := checkConfig(server, "", workload); err != nil {
		return nil, err
	}
	service, err := serviceAt(svcIP, svcPort)
	if err != nil {
		return nil, err
	}
	if os.Getenv("DPUMESH_SERVICE") != service {
		return nil, fmt.Errorf("dmesh: DPUMESH_SERVICE must be %q", service)
	}
	t, err := openTransport()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return nil, t.err
	}
	if t.listener != nil {
		return nil, syscall.EADDRINUSE
	}
	l := &Listener{t: t, addr: &net.TCPAddr{IP: net.ParseIP(svcIP), Port: svcPort}}
	t.listener = l
	return l, nil
}
func (l *Listener) Accept() (net.Conn, error) {
	for {
		l.t.mu.Lock()
		if l.closed {
			l.t.mu.Unlock()
			return nil, net.ErrClosed
		}
		if len(l.pending) != 0 {
			c := l.pending[0]
			l.pending[0] = nil
			l.pending = l.pending[1:]
			l.t.mu.Unlock()
			return c, nil
		}
		if l.t.err != nil {
			err := l.t.err
			l.t.mu.Unlock()
			return nil, err
		}
		ch := l.t.changed
		l.t.mu.Unlock()
		<-ch
	}
}
func (l *Listener) Close() error {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.t.listener = nil
	for _, c := range l.pending {
		c.closeLocked(true)
	}
	l.pending = nil
	l.t.notify()
	return nil
}
func (l *Listener) Addr() net.Addr { return l.addr }

// DialAddress opens a logical stream using the process registration. The DPU
// chooses a live backend for the service at the configured virtual address.
func DialAddress(ip string, port int) (net.Conn, error) {
	return Dial("", "", 0, ip, port, "")
}

// DialContext is the gRPC ContextDialer entry point. Native registration has
// its own bounded setup timeout; a cancelled caller never receives a live QP.
func DialContext(ctx context.Context, ip string, port int) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := DialAddress(ip, port)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// ListenAddress serves the process's DPUMESH_SERVICE identity.
func ListenAddress(ip string, port int) (*Listener, error) {
	return Listen("", ip, port, "")
}
