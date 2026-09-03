//go:build ignore

package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

var (
	easingLogBase *float64
	mixedPriority *bool
	workMode      *string
	maxProcs      *int
)

// spinItersPerMs is the number of arithmetic iterations that consume ~1ms of
// on-core CPU, calibrated once at startup when -work-mode=cpu. Each request then
// burns a FIXED amount of CPU work rather than sleeping, so a holder actually
// occupies a core for the duration of its "work". Under core contention the
// wall-clock to finish that fixed work stretches (the goroutine is preempted),
// which is the latency inflation and slot-refill delay the sleep model cannot
// produce — and the only regime where the keep-droppable floor has anything to do.
var spinItersPerMs uint64

// spinYieldIters is the busySpinCPU iteration count between yields, set at
// calibration to roughly spinYieldUs microseconds of on-core work.
var spinYieldIters uint64 = 1 << 20

// spinYieldUs is the target on-core interval between yields in busySpinCPU.
const spinYieldUs = 250

// calibrateSpin measures how many iterations fit in a short on-core probe and
// records the per-ms rate. Run once, before load starts, on a locked thread so
// the probe is not descheduled mid-measurement.
func calibrateSpin() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	const probe = 50 * time.Millisecond
	var acc uint64
	var iters uint64
	start := time.Now()
	for time.Since(start) < probe {
		for i := 0; i < 1024; i++ {
			acc = acc*1103515245 + 12345
		}
		iters += 1024
	}
	_ = acc
	elapsedMs := float64(time.Since(start).Nanoseconds()) / 1e6
	spinItersPerMs = uint64(float64(iters) / elapsedMs)
	if spinItersPerMs == 0 {
		spinItersPerMs = 1
	}
	// Yield roughly every spinYieldUs of on-core work.
	spinYieldIters = spinItersPerMs * spinYieldUs / 1000
	if spinYieldIters == 0 {
		spinYieldIters = 1
	}
}

// busySpinCPU burns approximately d of CPU work using the calibrated iteration
// rate. Unlike time.Sleep, this keeps the goroutine on a core (contending for
// P), so holders genuinely occupy backend concurrency for their work duration.
//
// It yields (runtime.Gosched) every spinYieldIters iterations so the workload
// does not monopolize its P for the whole quantum. Go's async preemption only
// fires at ~10ms, so a tight <10ms spin would otherwise run uninterrupted,
// starving the load generator of a core (offered load collapses to the service
// rate). Yielding creates scheduling points WITHOUT reducing CPU demand — the
// goroutine comes right back and still wants a full core — so structural
// saturation is preserved while intake still makes progress.
func busySpinCPU(d time.Duration) {
	ms := float64(d.Nanoseconds()) / 1e6
	total := uint64(ms * float64(spinItersPerMs))
	var acc uint64
	for i := uint64(0); i < total; i++ {
		acc = acc*1103515245 + 12345
		if i%spinYieldIters == spinYieldIters-1 {
			runtime.Gosched()
		}
	}
	_ = acc
}

type event struct {
	tsMs      int64
	kind      string // "issued", "granted", "shed"
	latencyMs float64
	priority  float64
}

type statsSnapshot struct {
	tsMs            int64
	queueLen        int
	droppableLen    int
	holderCount     int
	dropping        bool
	dropCount       int
	currentInterval int64 // ns
}

// loadProfile returns the load fraction [0,1] at a given elapsed time.
type loadProfile func(elapsed, totalDuration time.Duration) float64

// sineProfile oscillates the load fraction between floor and 1.0 over each
// period. floor=0 spans the full [0,1]; floor=0.5 spans [0.5,1] (so with
// peak=4x the load swings between a 2x trough and a 4x peak).
func sineProfile(period time.Duration, floor float64) loadProfile {
	return func(elapsed, _ time.Duration) float64 {
		phase := float64(elapsed) / float64(period) * 2 * math.Pi
		return floor + (1-floor)*(1-math.Cos(phase))/2
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

func linearRampDownProfile() loadProfile {
	return func(elapsed, totalDuration time.Duration) float64 {
		return 1 - float64(elapsed)/float64(totalDuration)
	}
}

// brownNoiseProfile returns a repeatable brown-noise (random-walk) load
// fraction in [0,1]. The walk is fully determined by seed, so the same seed
// reproduces the same offered-load trace. step controls volatility (the
// per-sample increment magnitude); the value reflects off the [0,1] bounds to
// stay in range. The walk is pre-sampled at sampleMs resolution and the
// profile interpolates between samples by elapsed time, so the trace is
// independent of how often the profile is queried.
func brownNoiseProfile(seed int64, step float64, sampleMs, durationMs int) loadProfile {
	rng := rand.New(rand.NewSource(seed))
	n := durationMs/sampleMs + 2
	samples := make([]float64, n)
	v := 0.5 // start mid-range
	for i := range samples {
		samples[i] = v
		v += (rng.Float64()*2 - 1) * step
		// reflect off [0,1] so the walk stays in range without clipping flat
		if v < 0 {
			v = -v
		} else if v > 1 {
			v = 2 - v
		}
	}
	sample := time.Duration(sampleMs) * time.Millisecond
	return func(elapsed, _ time.Duration) float64 {
		pos := float64(elapsed) / float64(sample)
		i := int(pos)
		if i >= len(samples)-1 {
			return samples[len(samples)-1]
		}
		frac := pos - float64(i)
		return samples[i]*(1-frac) + samples[i+1]*frac
	}
}

// profileOpts carries per-profile knobs for buildProfile.
type profileOpts struct {
	periodMs      int     // sine
	sineFloor     float64 // sine
	durationMs    int     // brown_noise (walk length)
	brownSeed     int64   // brown_noise
	brownStep     float64 // brown_noise volatility
	brownSampleMs int     // brown_noise sample resolution
}

// buildProfile constructs a loadProfile from a name and per-profile options.
func buildProfile(name string, o profileOpts) loadProfile {
	switch name {
	case "sine":
		return sineProfile(time.Duration(o.periodMs)*time.Millisecond, o.sineFloor)
	case "constant":
		return constantProfile()
	case "linear_ramp":
		return linearRampProfile()
	case "linear_ramp_down":
		return linearRampDownProfile()
	case "brown_noise":
		return brownNoiseProfile(o.brownSeed, o.brownStep, o.brownSampleMs, o.durationMs)
	default:
		fmt.Printf("unknown profile %q (expected sine|constant|linear_ramp|linear_ramp_down|brown_noise)\n", name)
		os.Exit(1)
		return nil
	}
}

func runBench(capacity int, peakArrivalRateMultiplier float64, durationMs, workMs, workStddevMs, targetMs, intervalMs int, profile loadProfile) ([]event, []statsSnapshot) {
	snake := loadshed.NewSnake[struct{}](loadshed.SnakeConfig{
		Name:     "bench",
		Capacity: func() int { return capacity },
		CoDel: loadshed.CoDelConfig{
			TargetNs:       func() int64 { return int64(targetMs) * 1_000_000 },
			IntervalNs:     func() int64 { return int64(intervalMs) * 1_000_000 },
			MinDropDelayNs: func() int64 { return 1_000_000 },
			Exponent:       func() float64 { return 1 },
			EasingLogBase:  func() float64 { return *easingLogBase },
		},
		Mode: func() loadshed.Mode { return loadshed.ModeEnabled },
	})

	totalDuration := time.Duration(durationMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), totalDuration+500*time.Millisecond)
	defer cancel()

	start := time.Now()

	var mu sync.Mutex
	var events []event

	record := func(kind string, latencyMs float64, priority float64) {
		ts := time.Since(start).Milliseconds()
		mu.Lock()
		events = append(events, event{tsMs: ts, kind: kind, latencyMs: latencyMs, priority: priority})
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

	// Work duration: fixed at workMs, or (when workStddevMs > 0) drawn per
	// request from a Gaussian N(workMs, workStddevMs) clamped to >= 0. The RNG
	// is seeded and mutex-guarded so a run is repeatable and goroutine-safe.
	workRng := rand.New(rand.NewSource(1))
	var workMu sync.Mutex
	sampleWork := func() time.Duration {
		ms := float64(workMs)
		if workStddevMs > 0 {
			workMu.Lock()
			ms = workRng.NormFloat64()*float64(workStddevMs) + float64(workMs)
			workMu.Unlock()
			if ms < 0 {
				ms = 0
			}
		}
		return time.Duration(ms * float64(time.Millisecond))
	}

	// Per-request priority. Default: all 0 (matches the original single-priority
	// workload). With -mixed-priority: uniform integer in [0,100], so the CoDel
	// queue holds a spread of droppable priorities and the lowest is rarely at
	// the front — exercising the full lowest-priority lookup rather than the
	// priority-0 early exit. Seeded + guarded for repeatability.
	prioRng := rand.New(rand.NewSource(2))
	var prioMu sync.Mutex
	samplePriority := func() float64 {
		if mixedPriority == nil || !*mixedPriority {
			return 0
		}
		prioMu.Lock()
		p := prioRng.Intn(101)
		prioMu.Unlock()
		return float64(p)
	}

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
					prio := samplePriority()
					record("issued", 0, prio)

					unlock, err := snake.Acquire(ctx, "", prio)
					if err != nil {
						record("shed", 0, prio)
						return
					}
					if workMode != nil && *workMode == "cpu" {
						busySpinCPU(sampleWork())
					} else {
						time.Sleep(sampleWork())
					}
					unlock.Release()
					latency := float64(time.Since(reqStart).Microseconds()) / 1000.0
					record("granted", latency, prio)
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
	workStddevMs              int
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
	outDir := flag.String("out", "", "Output directory (default: timestamped under ~/snake-load-test-charts/)")
	parallel := flag.Int("j", 8, "Max parallel benchmarks")
	filter := flag.String("filter", "", "Only run tests whose label contains this substring")
	easingLogBase = flag.Float64("easing-log-base", 3.0, "Log base for CoDel easing count decay")
	mixedPriority = flag.Bool("mixed-priority", false, "Issue requests with uniform priorities in [0,100] instead of all 0, so the lowest-priority drop lookup does not hit the priority-0 early exit")

	// Custom single-workload flags. When -profile is set, the workload described
	// by these flags REPLACES the preset sine/constant/ramp matrix. Otherwise the
	// preset matrix (with its built-in defaults) runs as before.
	wProfile := flag.String("profile", "", "Custom workload profile: sine|constant|linear_ramp|linear_ramp_down|brown_noise (replaces preset matrix when set)")
	wLabel := flag.String("label", "", "Custom workload label (default: derived from profile)")
	wCapacity := flag.Int("capacity", 10, "Custom workload: slot capacity")
	wPeak := flag.Float64("peak", 80, "Custom workload: peak arrival rate as multiple of system throughput")
	wDurationMs := flag.Int("duration-ms", 20000, "Custom workload: total duration in ms")
	wWorkMs := flag.Int("work-ms", 2, "Custom workload: per-request work duration in ms (mean)")
	wWorkStddevMs := flag.Int("work-stddev-ms", 0, "Custom workload: work duration stddev in ms; >0 draws each request's work from a Gaussian N(work-ms, stddev) clamped >=0")
	wTargetMs := flag.Int("target-ms", 5, "Custom workload: CoDel target in ms")
	wIntervalMs := flag.Int("interval-ms", 100, "Custom workload: CoDel interval in ms")
	wPeriodMs := flag.Int("period-ms", 1000, "Custom workload: sine period in ms (sine profile only)")
	wSineFloor := flag.Float64("sine-floor", 0, "Custom workload: sine trough as a fraction of peak (sine profile only); e.g. 0.5 => trough at half the peak load")
	wBrownSeed := flag.Int64("brown-seed", 1, "Custom workload: RNG seed for brown_noise profile (same seed => same trace)")
	wBrownStep := flag.Float64("brown-step", 0.05, "Custom workload: brown_noise per-sample volatility (random-walk increment magnitude)")
	wBrownSampleMs := flag.Int("brown-sample-ms", 100, "Custom workload: brown_noise walk sample resolution in ms")
	workMode = flag.String("work-mode", "sleep", "How simulated work is spent: sleep (time.Sleep, holder parks its P — no CPU pressure) or cpu (busy-spin the work duration of CPU, so holders occupy cores and freed slots contend to refill — the regime where the keep-droppable floor matters)")
	maxProcs = flag.Int("gomaxprocs", 0, "Override GOMAXPROCS for the run (0 = leave at runtime default). Set below capacity with -work-mode=cpu to saturate cores and produce scheduling-latency-driven underfill")
	flag.Parse()

	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}
	if *workMode == "cpu" {
		calibrateSpin()
		fmt.Printf("work-mode=cpu: calibrated %d spin-iters/ms, GOMAXPROCS=%d\n", spinItersPerMs, runtime.GOMAXPROCS(0))
	}

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

	// Custom single workload (replaces the preset matrix below).
	if *wProfile != "" {
		label := *wLabel
		if label == "" {
			label = *wProfile
		}
		configs = append(configs, testConfig{
			label:                     label,
			capacity:                  *wCapacity,
			peakArrivalRateMultiplier: *wPeak,
			durationMs:                *wDurationMs,
			workMs:                    *wWorkMs,
			workStddevMs:              *wWorkStddevMs,
			targetMs:                  *wTargetMs,
			intervalMs:                *wIntervalMs,
			profile: buildProfile(*wProfile, profileOpts{
				periodMs:      *wPeriodMs,
				sineFloor:     *wSineFloor,
				durationMs:    *wDurationMs,
				brownSeed:     *wBrownSeed,
				brownStep:     *wBrownStep,
				brownSampleMs: *wBrownSampleMs,
			}),
		})
		runConfigs(configs, *outDir, *parallel)
		return
	}

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
					profile:                   sineProfile(time.Duration(p.ms)*time.Millisecond, 0),
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

	runConfigs(configs, *outDir, *parallel)
}

// runConfigs runs the given benchmark configs in parallel and writes their
// event/stats TSVs under outDir/tsv/.
func runConfigs(configs []testConfig, outDir string, parallel int) {
	fmt.Printf("Running %d benchmarks (parallelism=%d)...\n", len(configs), parallel)

	results := make([]testResult, len(configs))
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup

	for i, cfg := range configs {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, c testConfig) {
			defer wg.Done()
			defer func() { <-sem }()

			fmt.Printf("  START [%d/%d] %s\n", idx+1, len(configs), c.label)
			events, stats := runBench(c.capacity, c.peakArrivalRateMultiplier, c.durationMs, c.workMs, c.workStddevMs, c.targetMs, c.intervalMs, c.profile)

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
	tsvDir := fmt.Sprintf("%s/tsv", outDir)
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
		csvW.Write([]string{"ts_ms", "event", "latency_ms", "priority"})
		for _, ev := range r.events {
			csvW.Write([]string{fmt.Sprintf("%d", ev.tsMs), ev.kind, fmt.Sprintf("%.3f", ev.latencyMs), fmt.Sprintf("%.0f", ev.priority)})
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
		csvS.Write([]string{"ts_ms", "queue_len", "droppable_len", "holder_count", "dropping", "drop_count", "current_interval_ns"})
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
			})
		}
		csvS.Flush()
		sf.Close()
	}

	fmt.Printf("\nDone. TSV files in %s\n", outDir)
	fmt.Printf("To generate charts: python3 plot_suite.py %s\n", outDir)
}
