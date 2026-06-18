//go:build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type event struct {
	tsMs      int64
	eventType string
	latencyMs float64
}

type statsSnapshot struct {
	tsMs            int64
	queueLen        int
	droppableLen    int
	holderCount     int
	dropping        bool
	dropCount       int
	currentInterval int64
}

type loadProfile func(elapsed, total time.Duration) float64

func linearRamp(elapsed, total time.Duration) float64 {
	return elapsed.Seconds() / total.Seconds()
}

func runTest(cfg testConfig) ([]event, []statsSnapshot) {
	capacity := cfg.capacity
	workMs := cfg.workMs
	targetMs := cfg.targetMs
	intervalMs := cfg.intervalMs
	exponent := cfg.exponent
	totalDuration := time.Duration(cfg.durationMs) * time.Millisecond
	peakArrivalRateMultiplier := cfg.peakArrivalRateMultiplier
	profile := cfg.profile

	snake := loadshed.NewSnake(loadshed.SnakeConfig{
		Name: "bench",
		CoDel: loadshed.CoDelConfig{
			TargetNs:       func() int64 { return int64(targetMs) * 1_000_000 },
			IntervalNs:     func() int64 { return int64(intervalMs) * 1_000_000 },
			MinDropDelayNs: func() int64 { return 1_000_000 },
			Exponent:       func() float64 { return exponent },
		},
		Capacity:            func() int { return capacity },
		MaxAge:              func() time.Duration { return 30 * time.Second },
		LoadsheddingAllowed: func() bool { return true },
	})

	ctx, cancel := context.WithCancel(context.Background())

	var mu sync.Mutex
	var events []event
	var stats []statsSnapshot
	start := time.Now()

	record := func(evtType string, latMs float64) {
		ts := time.Since(start).Milliseconds()
		mu.Lock()
		events = append(events, event{tsMs: ts, eventType: evtType, latencyMs: latMs})
		mu.Unlock()
	}

	// Stats polling goroutine
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := snake.Stats()
				ts := time.Since(start).Milliseconds()
				mu.Lock()
				stats = append(stats, statsSnapshot{
					tsMs:            ts,
					queueLen:        s.QueueLen,
					droppableLen:    s.DroppableLen,
					holderCount:     s.HolderCount,
					dropping:        s.Dropping,
					dropCount:       s.DropCount,
					currentInterval: s.CurrentInterval,
				})
				mu.Unlock()
			}
		}
	}()

	var wg sync.WaitGroup
	var issued atomic.Int64

	systemThroughput := float64(capacity) * 1000.0 / float64(workMs)
	peakArrivalRate := peakArrivalRateMultiplier * systemThroughput

	tickInterval := 1 * time.Millisecond
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	workDur := time.Duration(workMs) * time.Millisecond
	var accumulator float64

	deadline := time.After(totalDuration)
	issuing := true

	for issuing {
		select {
		case <-deadline:
			issuing = false
		case now := <-ticker.C:
			elapsed := now.Sub(start)
			loadFraction := profile(elapsed, totalDuration)

			arrivalsPerSec := loadFraction * peakArrivalRate
			arrivalsThisTick := arrivalsPerSec * tickInterval.Seconds()
			accumulator += arrivalsThisTick

			toSpawn := int(accumulator)
			accumulator -= float64(toSpawn)

			for range toSpawn {
				wg.Add(1)
				issued.Add(1)
				go func() {
					defer wg.Done()

					reqStart := time.Now()
					record("issued", 0)

					unlock, err := snake.Acquire(ctx, "")
					if err != nil {
						record("shed", 0)
						return
					}
					time.Sleep(workDur)
					unlock.Release()
					latency := float64(time.Since(reqStart).Microseconds()) / 1000.0
					record("granted", latency)
				}()
			}
		}
	}

	cancel()
	wg.Wait()
	<-statsDone
	return events, stats
}

type testConfig struct {
	label                     string
	capacity                  int
	peakArrivalRateMultiplier float64
	durationMs                int
	workMs                    int
	targetMs                  int
	intervalMs                int
	exponent                  float64
	profile                   loadProfile
}

func main() {
	outDir := flag.String("out", "", "Output directory for TSVs (default: ~/snake-load-test-charts/tsv/<date>/<timestamp>/)")
	exponent := flag.Float64("exponent", 1.0, "CoDel exponent (production default: 1.0)")
	targetMs := flag.Int("target-ms", 5, "CoDel target in ms (production default: 5)")
	intervalMs := flag.Int("interval-ms", 100, "CoDel interval in ms (production default: 100)")
	flag.Parse()

	if *outDir == "" {
		now := time.Now()
		date := now.Format("2006-01-02")
		ts := now.Format("2006-01-02_15-04-05")
		home, _ := os.UserHomeDir()
		*outDir = filepath.Join(home, "snake-load-test-charts", "tsv", date, ts)
	}
	os.MkdirAll(*outDir, 0o755)

	tests := []testConfig{
		{
			label:                     "linear_ramp__0_to_80x_cap__work_half_target",
			capacity:                  10,
			peakArrivalRateMultiplier: 80,
			durationMs:                20000,
			workMs:                    2,
			targetMs:                  *targetMs,
			intervalMs:                *intervalMs,
			exponent:                  *exponent,
			profile:                   linearRamp,
		},
	}

	for _, tc := range tests {
		fmt.Printf("Running: %s (capacity=%d, work=%dms, target=%dms, interval=%dms, exponent=%.2f)\n",
			tc.label, tc.capacity, tc.workMs, tc.targetMs, tc.intervalMs, tc.exponent)
		events, stats := runTest(tc)

		// Write events TSV
		tsvPath := filepath.Join(*outDir, tc.label+".tsv")
		f, err := os.Create(tsvPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR creating %s: %v\n", tsvPath, err)
			continue
		}
		fmt.Fprintf(f, "ts_ms\tevent\tlatency_ms\n")
		for _, e := range events {
			latStr := ""
			if e.eventType == "granted" {
				latStr = fmt.Sprintf("%.3f", e.latencyMs)
			}
			fmt.Fprintf(f, "%d\t%s\t%s\n", e.tsMs, e.eventType, latStr)
		}
		f.Close()

		// Write stats TSV
		statsPath := filepath.Join(*outDir, tc.label+"_stats.tsv")
		sf, err := os.Create(statsPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR creating %s: %v\n", statsPath, err)
			continue
		}
		fmt.Fprintf(sf, "ts_ms\tqueue_len\tdroppable_len\tholder_count\tdropping\tdrop_count\tcurrent_interval_ns\n")
		for _, s := range stats {
			droppingInt := 0
			if s.dropping {
				droppingInt = 1
			}
			fmt.Fprintf(sf, "%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
				s.tsMs, s.queueLen, s.droppableLen, s.holderCount, droppingInt, s.dropCount, s.currentInterval)
		}
		sf.Close()

		var issuedCount, grantedCount, shedCount int
		for _, e := range events {
			switch e.eventType {
			case "issued":
				issuedCount++
			case "granted":
				grantedCount++
			case "shed":
				shedCount++
			}
		}
		shedPct := float64(shedCount) / math.Max(float64(issuedCount), 1) * 100
		fmt.Printf("  %s: issued=%d granted=%d shed=%d (%.1f%%)\n",
			tc.label, issuedCount, grantedCount, shedCount, shedPct)
	}

	fmt.Printf("\nOutput: %s\n", *outDir)
}
