# Snake Benchmark Suite

Exercises Snake under synthetic load to visualize CoDel behavior (grant rate, shedding,
queue depth, interval adaptation, latency). Outputs TSV files consumed by the plotting
scripts.

## Running the bench

```bash
cd go/vt/vttablet/tabletserver/loadshed/benchmark

# With defaults:
go run bench_suite.go

# Override easing divisor:
go run bench_suite.go -easing 1.2

# Only run linear ramp tests:
go run bench_suite.go -filter linear_ramp

# Custom output directory + parallelism:
go run bench_suite.go -out /tmp/my-run -j 4
```

Output lands in `~/snake-load-test-charts/<timestamp>/tsv/` by default.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-out` | auto | Output directory for TSVs |
| `-j` | 8 | Max parallel benchmarks |
| `-filter` | (none) | Only run tests whose label contains this substring |
| `-easing` | 2.0 | Easing divisor for CoDel count decay |

### Custom single workload

When `-profile` is set, the named workload **replaces** the preset
sine/constant/ramp matrix. Otherwise the preset matrix runs with its built-in
defaults (unchanged behavior).

```bash
go run bench_suite.go -profile linear_ramp -peak 80 -duration-ms 20000 \
  -work-ms 2 -target-ms 5 -interval-ms 100 -label my_ramp
```

| Flag | Default | Description |
|------|---------|-------------|
| `-profile` | (none) | `sine`, `constant`, `linear_ramp`, or `linear_ramp_down` (replaces preset matrix when set) |
| `-label` | profile name | TSV label for the workload |
| `-capacity` | 10 | Slot capacity |
| `-peak` | 80 | Peak arrival rate as multiple of system throughput |
| `-duration-ms` | 20000 | Total duration |
| `-work-ms` | 2 | Per-request work duration |
| `-target-ms` | 5 | CoDel target |
| `-interval-ms` | 100 | CoDel interval |
| `-period-ms` | 1000 | Sine period (sine profile only) |

## Plotting

Requires Python 3 with matplotlib, pandas, and numpy:

```bash
# Plot the full suite (sine, constant, linear ramp with CoDel internals):
python3 plot_suite.py ~/snake-load-test-charts/<timestamp>

# Plot a linear ramp run:
python3 plot_linear_ramp.py ~/snake-load-test-charts/<timestamp>/tsv/
```

## Easing comparison

Compares CoDel ease-out behavior across divisor values (how aggressively count
decays when the queue drains). Runs all divisors in parallel, then plots:

```bash
python3 plot_easing_comparison.py --run
```

Or plot from existing TSVs without re-running benchmarks:

```bash
python3 plot_easing_comparison.py
```

| Flag | Default | Description |
|------|---------|-------------|
| `--run` | off | Run Go bench suite for all divisors in parallel before plotting |
| `--bench-go` | `bench_suite.go` | Path to bench_suite.go |
| `--filter` | `linear_ramp__0_to_80x_cap__work_half_target` | Only run tests matching this label |
| `--config` | (none) | JSON file describing `divisors` + `workloads` (see below) |

### Config-driven comparison

With `--config <file.json>`, the script runs every (divisor × workload)
combination in parallel, then emits one comparison figure per workload. Any
workload field omitted falls back to the defaults. See
`easing_config.example.json`.

```bash
python3 plot_easing_comparison.py --config easing_config.example.json
```

```json
{
  "divisors": [1.189, 1.260, 1.414, 2.0],
  "workloads": [
    {"label": "ramp", "profile": "linear_ramp", "peak": 80, "duration_ms": 20000,
     "work_ms": 2, "target_ms": 5, "interval_ms": 100},
    {"label": "sine", "profile": "sine", "peak": 100, "period_ms": 1000}
  ]
}
```
