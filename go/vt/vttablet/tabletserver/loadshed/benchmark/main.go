package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type record struct {
	ts      time.Duration
	granted bool
}

var (
	mu      sync.Mutex
	records []record
	start   time.Time
	logPath string
)

func flushLog() {
	if logPath == "" {
		return
	}
	f, err := os.Create(logPath)
	if err != nil {
		log.Printf("failed to create log file: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "ts_ms\tresult\n")
	mu.Lock()
	defer mu.Unlock()
	for _, rec := range records {
		result := "granted"
		if !rec.granted {
			result = "shed"
		}
		fmt.Fprintf(f, "%d\t%s\n", rec.ts.Milliseconds(), result)
	}
	log.Printf("wrote %d records to %s", len(records), logPath)
}

func main() {
	port := flag.Int("port", 8765, "HTTP listen port")
	capacity := flag.Int("capacity", 10, "Snake semaphore capacity")
	targetMs := flag.Int("target", 5, "CoDel target delay (ms)")
	intervalMs := flag.Int("interval", 100, "CoDel observation interval (ms)")
	workMs := flag.Int("work", 5, "Default simulated work duration (ms)")
	logFileFlag := flag.String("log", "", "Path to write per-request TSV log (ts_ms, granted|shed)")
	flag.Parse()

	logPath = *logFileFlag

	snake := loadshed.NewSnake(loadshed.SnakeConfig{
		Name:     "bench",
		Capacity: func() int { return *capacity },
		MaxAge:   func() time.Duration { return 30 * time.Second },
		CoDel: loadshed.CoDelConfig{
			TargetNs:       func() int64 { return int64(*targetMs) * 1_000_000 },
			IntervalNs:     func() int64 { return int64(*intervalMs) * 1_000_000 },
			MinDropDelayNs: func() int64 { return 1_000_000 },
			Exponent:       func() float64 { return 0.5 },
		},
		LoadsheddingAllowed: func() bool { return true },
	})

	if logPath != "" {
		start = time.Now()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sig
			flushLog()
			os.Exit(0)
		}()
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		work := time.Duration(*workMs) * time.Millisecond
		if q := r.URL.Query().Get("work_ms"); q != "" {
			if v, err := strconv.Atoi(q); err == nil {
				work = time.Duration(v) * time.Millisecond
			}
		}

		valveID := r.URL.Query().Get("valve_id")

		unlock, err := snake.Acquire(context.Background(), valveID)
		if err != nil {
			if logPath != "" {
				mu.Lock()
				records = append(records, record{ts: time.Since(start), granted: false})
				mu.Unlock()
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "shed: %v\n", err)
			return
		}
		defer unlock.Release()

		time.Sleep(work)

		if logPath != "" {
			mu.Lock()
			records = append(records, record{ts: time.Since(start), granted: true})
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok\n")
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("snakeserver listening on %s (capacity=%d, target=%dms, interval=%dms, work=%dms, log=%s)",
		addr, *capacity, *targetMs, *intervalMs, *workMs, logPath)
	log.Fatal(http.ListenAndServe(addr, nil))
}
