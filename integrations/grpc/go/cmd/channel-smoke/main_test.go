package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type failingCloseConn struct {
	net.Conn
	err   error
	calls atomic.Int32
}

func (c *failingCloseConn) Close() error {
	c.calls.Add(1)
	return c.err
}

func closeTestPeer(t *testing.T) *peer {
	t.Helper()
	// NewClient is lazy; no Connect or RPC means no socket or DOCA device opens.
	cc, err := grpc.NewClient("passthrough:///close-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return &peer{conn: cc, label: "close-test", closePending: 1, closeChanged: make(chan struct{})}
}

func TestPeerCloseReportsNativeError(t *testing.T) {
	p := closeTestPeer(t)
	injected := errors.New("native teardown failed")
	native := &failingCloseConn{err: injected}
	conn := &observedConn{Conn: native, owner: p}
	for i := 0; i < 2; i++ {
		if err := conn.Close(); !errors.Is(err, injected) {
			t.Fatalf("native close = %v, want injected failure", err)
		}
	}
	if native.calls.Load() != 1 {
		t.Fatal("native close was retried despite the consumed QP contract")
	}
	if err := p.close(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("peer close hid native teardown error: %v", err)
	}
}

func TestPeerCloseWaitIsBounded(t *testing.T) {
	p := closeTestPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pending native close = %v, want context cancellation", err)
	}
}
