package dmeshgo

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// These cases use empty native handles, for which close is explicitly a no-op.
// They test Go wait/cancellation behavior without registering a DOCA device.
func idleConn() *Conn { return &Conn{t: &transport{changed: make(chan struct{})}} }
func awaitError(t *testing.T, done <-chan error, expected error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, expected) {
			t.Fatalf("got %v, want %v", err, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked operation was not woken")
	}
}
func TestReadDeadlineAndCloseWakeWaiters(t *testing.T) {
	for _, closeIt := range []bool{false, true} {
		c := idleConn()
		done := make(chan error, 1)
		go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
		if closeIt {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, net.ErrClosed)
			if err := c.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		} else {
			if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, os.ErrDeadlineExceeded)
			// Clearing a deadline must allow a later read to wait for data/EOF.
			if err := c.SetReadDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
			go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
			c.t.mu.Lock()
			c.eof = true
			c.t.notify()
			c.t.mu.Unlock()
			awaitError(t, done, io.EOF)
		}
	}
}
func TestWriteDeadlineAndListenerCancellation(t *testing.T) {
	c := idleConn()
	if err := c.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("%d, %v", n, err)
	}
	l := &Listener{t: c.t}
	c.t.listener = l
	done := make(chan error, 1)
	go func() { _, err := l.Accept(); done <- err }()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	awaitError(t, done, net.ErrClosed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if conn, err := DialContext(ctx, "invalid", -1); conn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("%v, %v", conn, err)
	}
}
