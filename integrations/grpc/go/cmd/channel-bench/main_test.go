package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFixedTotalConcurrency(t *testing.T) {
	for connections, want := range map[int][]int{1: {64}, 2: {32, 32}, 3: {22, 21, 21}, 4: {16, 16, 16, 16}} {
		if got := distribute(64, connections); !reflect.DeepEqual(got, want) {
			t.Fatalf("connections=%d distribution=%v, want %v", connections, got, want)
		}
	}
}

func TestCompletionMeasurementBoundaries(t *testing.T) {
	start := time.Now()
	end := start.Add(10 * time.Second)
	for _, tc := range []struct {
		at   time.Time
		want bool
	}{
		{start.Add(-time.Nanosecond), false},
		{start, true},
		{end.Add(-time.Nanosecond), true},
		{end, false},
		{end.Add(time.Nanosecond), false},
	} {
		if got := inWindow(tc.at, start, end); got != tc.want {
			t.Errorf("completion offset %v: got %v, want %v", tc.at.Sub(start), got, tc.want)
		}
	}
}

func TestLatencyIncludesTailOfEveryWorker(t *testing.T) {
	// Late high-latency completions must remain represented, without a first-N cap.
	latencies := make([]time.Duration, 100)
	for i := range latencies {
		latencies[i] = time.Microsecond
	}
	latencies[98], latencies[99] = 100*time.Microsecond, 1000*time.Microsecond
	mean, p50, p99 := summarize(latencies)
	if mean != 11.98 || p50 != 1 || p99 != 100 {
		t.Fatalf("summary = %v/%v/%v us, want 11.98/1/100", mean, p50, p99)
	}
}

type failingCloseConn struct {
	net.Conn
	err   error
	calls atomic.Int32
}

func (c *failingCloseConn) Close() error {
	c.calls.Add(1)
	return c.err
}

func TestNativeCloseErrorIsRetainedAndQPNotRetried(t *testing.T) {
	injected := errors.New("native close failure")
	native := &failingCloseConn{err: injected}
	tracker := newCloseTracker()
	conn := tracker.observe(native)
	for i := 0; i < 2; i++ {
		if err := conn.Close(); !errors.Is(err, injected) {
			t.Fatalf("close = %v, want injected failure", err)
		}
	}
	if native.calls.Load() != 1 {
		t.Fatal("consumed native QP was retried")
	}
	if err := tracker.wait(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("tracker wait = %v, want injected failure", err)
	}
}

func TestLoadStartTimestamp(t *testing.T) {
	now := time.Now()
	want := now.Add(time.Second)
	got, err := parseLoadStart([]byte(want.Format(time.RFC3339Nano)+"\n"), now)
	if err != nil || !got.Equal(want) || got.Sub(now) != time.Second {
		t.Fatalf("start = %v, %v; want %v", got, err, want)
	}
	for _, input := range []string{
		"", "not a timestamp", now.Format(time.RFC3339Nano), now.Add(-time.Nanosecond).Format(time.RFC3339Nano),
	} {
		if _, err := parseLoadStart([]byte(input), now); err == nil {
			t.Errorf("accepted invalid or non-future start %q", input)
		}
	}
}

func TestMissingStartFileWaitIsCancelable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err := waitForStartFile(ctx, filepath.Join(t.TempDir(), "not-published"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing start file wait = %v, want deadline exceeded", err)
	}
}

func TestStartFileRejectsInvalidOrPastTimestamp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "start")
	for _, input := range []string{"invalid", time.Now().Add(-time.Hour).Format(time.RFC3339Nano)} {
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := waitForStartFile(context.Background(), path); err == nil || !strings.Contains(err.Error(), path) {
			t.Fatalf("start file %q: error = %v, want immediate timestamp error with filename", input, err)
		}
	}
}

func TestStartFilePublishedWhileWaiting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "start")
	want := time.Now().Add(time.Hour)
	type outcome struct {
		start time.Time
		err   error
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan outcome, 1)
	go func() {
		start, err := waitForStartFile(ctx, path)
		done <- outcome{start, err}
	}()
	staged := filepath.Join(dir, "staged")
	if err := os.WriteFile(staged, []byte(want.Format(time.RFC3339Nano)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	got := <-done
	// The published timestamp carries wall time only. Projecting it onto a
	// separately sampled monotonic clock can differ by a nanosecond, so compare
	// the shared controller time rather than the private monotonic readings.
	if got.err != nil || !got.start.Round(0).Equal(want.Round(0)) {
		t.Fatalf("published start = %v, %v; want %v", got.start, got.err, want)
	}
	// Cancellation wins even when a valid start file already exists.
	cancel()
	if _, err := waitForStartFile(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled start file wait = %v, want context canceled", err)
	}
}

func TestScheduledStartWaitIsCancelable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitUntil(ctx, time.Now().Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatalf("scheduled start wait = %v, want context canceled", err)
	}
}
