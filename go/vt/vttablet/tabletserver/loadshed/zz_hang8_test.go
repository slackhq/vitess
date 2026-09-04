package loadshed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHang8_RealLoad_MinDropDelay1s(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 4 }
	cfg.CoDel.IntervalNs = func() int64 { return 100_000_000 } // 100ms
	cfg.CoDel.TargetNs = func() int64 { return 5_000_000 }     // 5ms
	cfg.CoDel.Exponent = func() float64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000_000_000 } // 1s (the reported setting)
	s := NewSnake[struct{}](cfg)

	var granted, shed atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(6 * time.Second)
	// sustained overload: many workers hammering Acquire/Release
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				u, err := s.Acquire(ctx, "", 0)
				if err != nil {
					shed.Add(1)
					cancel()
					continue
				}
				granted.Add(1)
				time.Sleep(2 * time.Millisecond)
				u.Release()
				cancel()
			}
		}()
	}
	// watchdog: if Stats() blocks (s.mu held by a spin), we detect the hang
	hung := make(chan struct{}, 1)
	go func() {
		for time.Now().Before(deadline.Add(2 * time.Second)) {
			done := make(chan struct{}, 1)
			go func() { _ = s.Stats(); done <- struct{}{} }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				hung <- struct{}{}
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-hung:
		t.Fatalf("HANG detected: Stats() blocked >2s (s.mu held). granted=%d shed=%d", granted.Load(), shed.Load())
	case <-waitDone:
		t.Logf("no hang: granted=%d shed=%d", granted.Load(), shed.Load())
	case <-time.After(12 * time.Second):
		t.Fatalf("workers did not finish (likely wedged): granted=%d shed=%d", granted.Load(), shed.Load())
	}
}
