//go:build ignore

package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

var easingDiv *float64

type event struct {
	tsMs      int64
	kind      string // "issued", "granted", "shed"
	latencyMs float64
}

type statsSnapshot struct {
	tsMs            int64
	queueLen        int
	droppableLen    int
	holderCount     int
	dropping        bool
	dropCount       int
	currentInterval int64 // ns
	lastDropsPerRun int
}

// loadProfile returns the load fraction [0,1] at a given elapsed time.
type loadProfile func(elapsed, totalDuration time.Duration) float64

func sineProfile(period time.Duration) loadProfile {
	return func(elapsed, _ time.Duration) float64 {
		phase := float64(elapsed) / float64(period) * 2 * math.Pi
		return (1 - math.Cos(phase)) / 2
	}
}

func constantProfile() loadProfile {
	return func(_, _ time.Duration) float64 {
		return 1.0
	}
}

func linearRampProfile() loadProfile {
	return func(elapsed, totalDuration time.Duration) float64 {
		return float64(elapsed) / float64(totalDuration)
	}
}

func runBench(capacity int, peakArrivalRateMultiplier float64, durationMs, workMs, targetMs, intervalMs int, profile loadProfile) ([]event, []statsSnapshot) {
	snake := loadshed.NewSnake(loadshed.SnakeConfig{
		Name:     "bench",
		Capacity: func() int { return capacity },
		MaxAge:   func() time.Duration { return 30 * time.Second },
		CoDel: loadshed.CoDelConfig{
			TargetNs:       func() int64 { return int64(targetMs) * 1_000_000 },
			IntervalNs:     func() int64 { return int64(intervalMs) * 1_000_000 },
			MinDropDelayNs: func() int64 { return 1_000_000 },
			Exponent:       func() float64 { return 1 },
			EasingDivisor:  func() float64 { return *easingDiv },
		},
		LoadsheddingAllowed: func() bool { return true },
	})

	totalDuration := time.Duration(durationMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), totalDuration+500*time.Millisecond)
	defer cancel()

	start := time.Now()

	var mu sync.Mutex
	var events []event

	record := func(kind string, latencyMs float64) {
		ts := time.Since(start).Milliseconds()
		mu.Lock()
		events = append(events, event{tsMs: ts, kind: kind, latencyMs: latencyMs})
		mu.Unlock()
	}

	// Stats sampling goroutine — sample every 5ms
	var stats []statsSnapshot
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		statsTicker := time.NewTicker(5 * time.Millisecond)
		defer statsTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				s := snake.Stats()
				stats = append(stats, statsSnapshot{
					tsMs:            time.Since(start).Milliseconds(),
					queueLen:        s.QueueLen,
					droppableLen:    s.DroppableLen,
					holderCount:     s.HolderCount,
					dropping:        s.Dropping,
					dropCount:       s.DropCount,
					currentInterval: s.CurrentInterval,
					lastDropsPerRun: s.LastDropsPerRun,
				})
			}
		}
	}()

	var wg sync.WaitGroup

	// Peak arrival rate as multiple of system throughput
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

	// Cancel context to flush queued requests, then wait for drain
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
	profile                   loadProfile
}

type testResult struct {
	cfg                   testConfig
	issued, granted, shed int
	events                []event
	stats                 []statsSnapshot
}

func main() {
	outDir := flag.String("out", "", "Output directory (default: timestamped under ~/claude-context-store/snake-load-test-charts/)")
	parallel := flag.Int("j", 8, "Max parallel benchmarks")
	filter := flag.String("filter", "", "Only run tests whose label contains this substring")
	easingDiv = flag.Float64("easing", 2.0, "Easing divisor for CoDel count decay")
	flag.Parse()

	if *outDir == "" {
		home, _ := os.UserHomeDir()
		*outDir = fmt.Sprintf("%s/snake-load-test-charts/%s", home, time.Now().Format("2006-01-02_15-04-05"))
	}
	os.MkdirAll(*outDir, 0755)

	capacity := 10
	targetMs := 5
	intervalMs := 100

	works := []struct {
		label string
		ms    int
	}{
		{"half_target", targetMs / 2},
		{"equal_target", targetMs},
	}

	var configs []testConfig

	// Sine wave tests: 3 periods × 3 peaks × 2 work durations, duration = 2 full periods
	periods := []struct {
		label string
		ms    int
	}{
		{"2x_interval", 200},
		{"10x_interval", 1000},
		{"20x_interval", 2000},
	}

	peaks := []struct {
		label      string
		multiplier float64
	}{
		{"20x_cap", 20},
		{"100x_cap", 100},
		{"300x_cap", 300},
	}

	for _, p := range periods {
		for _, pk := range peaks {
			for _, w := range works {
				label := fmt.Sprintf("sine__period_%s__peak_%s__work_%s", p.label, pk.label, w.label)
				configs = append(configs, testConfig{
					label:                     label,
					capacity:                  capacity,
					peakArrivalRateMultiplier: pk.multiplier,
					durationMs:                p.ms * 2,
					workMs:                    w.ms,
					targetMs:                  targetMs,
					intervalMs:                intervalMs,
					profile:                   sineProfile(time.Duration(p.ms) * time.Millisecond),
				})
			}
		}
	}

	// Constant load at 5x capacity, 20s
	for _, w := range works {
		label := fmt.Sprintf("constant__5x_cap__work_%s", w.label)
		configs = append(configs, testConfig{
			label:                     label,
			capacity:                  capacity,
			peakArrivalRateMultiplier: 5,
			durationMs:                20000,
			workMs:                    w.ms,
			targetMs:                  targetMs,
			intervalMs:                intervalMs,
			profile:                   constantProfile(),
		})
	}

	// Linear ramp from 0x to 80x capacity, 20s
	for _, w := range works {
		label := fmt.Sprintf("linear_ramp__0_to_80x_cap__work_%s", w.label)
		configs = append(configs, testConfig{
			label:                     label,
			capacity:                  capacity,
			peakArrivalRateMultiplier: 80,
			durationMs:                20000,
			workMs:                    w.ms,
			targetMs:                  targetMs,
			intervalMs:                intervalMs,
			profile:                   linearRampProfile(),
		})
	}

	if *filter != "" {
		var filtered []testConfig
		for _, c := range configs {
			if strings.Contains(c.label, *filter) {
				filtered = append(filtered, c)
			}
		}
		configs = filtered
	}

	fmt.Printf("Running %d benchmarks (parallelism=%d)...\n", len(configs), *parallel)

	results := make([]testResult, len(configs))
	sem := make(chan struct{}, *parallel)
	var wg sync.WaitGroup

	for i, cfg := range configs {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, c testConfig) {
			defer wg.Done()
			defer func() { <-sem }()

			fmt.Printf("  START [%d/%d] %s\n", idx+1, len(configs), c.label)
			events, stats := runBench(c.capacity, c.peakArrivalRateMultiplier, c.durationMs, c.workMs, c.targetMs, c.intervalMs, c.profile)

			var issued, granted, shed int
			for _, ev := range events {
				switch ev.kind {
				case "issued":
					issued++
				case "granted":
					granted++
				case "shed":
					shed++
				}
			}
			results[idx] = testResult{cfg: c, issued: issued, granted: granted, shed: shed, events: events, stats: stats}
			shedRate := 0.0
			if issued > 0 {
				shedRate = float64(shed) / float64(issued) * 100
			}
			fmt.Printf("  DONE  [%d/%d] %s → %d issued, %d granted, %d shed (%.1f%%)\n",
				idx+1, len(configs), c.label, issued, granted, shed, shedRate)
		}(i, cfg)
	}

	wg.Wait()

	// Write TSV files into a tsv/ subdirectory
	tsvDir := fmt.Sprintf("%s/tsv", *outDir)
	os.MkdirAll(tsvDir, 0755)
	for _, r := range results {
		path := fmt.Sprintf("%s/%s.tsv", tsvDir, r.cfg.label)
		f, err := os.Create(path)
		if err != nil {
			fmt.Printf("  ERROR writing %s: %v\n", path, err)
			continue
		}
		csvW := csv.NewWriter(f)
		csvW.Comma = '\t'
		csvW.Write([]string{"ts_ms", "event", "latency_ms"})
		for _, ev := range r.events {
			csvW.Write([]string{fmt.Sprintf("%d", ev.tsMs), ev.kind, fmt.Sprintf("%.3f", ev.latencyMs)})
		}
		csvW.Flush()
		f.Close()

		// Write stats TSV
		statsPath := fmt.Sprintf("%s/%s_stats.tsv", tsvDir, r.cfg.label)
		sf, err := os.Create(statsPath)
		if err != nil {
			fmt.Printf("  ERROR writing %s: %v\n", statsPath, err)
			continue
		}
		csvS := csv.NewWriter(sf)
		csvS.Comma = '\t'
		csvS.Write([]string{"ts_ms", "queue_len", "droppable_len", "holder_count", "dropping", "drop_count", "current_interval_ns", "last_drops_per_run"})
		for _, s := range r.stats {
			dropping := "0"
			if s.dropping {
				dropping = "1"
			}
			csvS.Write([]string{
				fmt.Sprintf("%d", s.tsMs),
				fmt.Sprintf("%d", s.queueLen),
				fmt.Sprintf("%d", s.droppableLen),
				fmt.Sprintf("%d", s.holderCount),
				dropping,
				fmt.Sprintf("%d", s.dropCount),
				fmt.Sprintf("%d", s.currentInterval),
				fmt.Sprintf("%d", s.lastDropsPerRun),
			})
		}
		csvS.Flush()
		sf.Close()
	}

	fmt.Printf("\nDone. TSV files in %s\n", *outDir)
	fmt.Printf("To generate charts: python3 plot_suite.py %s\n", *outDir)
}
