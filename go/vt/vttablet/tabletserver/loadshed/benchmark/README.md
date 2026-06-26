# Snake Benchmark Suite

Exercises Snake under synthetic load to visualize CoDel behavior (grant rate, shedding,
queue depth, interval adaptation, latency). Outputs TSV files consumed by the plotting
scripts.

## Running the bench

```bash
cd go/vt/vttablet/tabletserver/loadshed/benchmark

# With defaults:
go run bench_suite.go

# Override easing log base:
go run bench_suite.go -easing-log-base 3

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
| `-easing-log-base` | 3.0 | Log base for CoDel easing count decay: `count -= floor(log_base(count)/base)` |

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
| `-profile` | (none) | `sine`, `constant`, `linear_ramp`, `linear_ramp_down`, or `brown_noise` (replaces preset matrix when set) |
| `-label` | profile name | TSV label for the workload |
| `-capacity` | 10 | Slot capacity |
| `-peak` | 80 | Peak arrival rate as multiple of system throughput |
| `-duration-ms` | 20000 | Total duration |
| `-work-ms` | 2 | Per-request work duration (mean) |
| `-work-stddev-ms` | 0 | Work-duration stddev; >0 draws each request's work from a Gaussian `N(work-ms, stddev)` clamped `>=0` |
| `-target-ms` | 5 | CoDel target |
| `-interval-ms` | 100 | CoDel interval |
| `-period-ms` | 1000 | Sine period (sine profile only) |
| `-sine-floor` | 0 | Sine trough as a fraction of peak, e.g. `0.5` => trough at half peak (sine only) |
| `-brown-seed` | 1 | RNG seed for `brown_noise`; same seed => same offered-load trace |
| `-brown-step` | 0.05 | `brown_noise` per-sample volatility (random-walk increment) |
| `-brown-sample-ms` | 100 | `brown_noise` walk sample resolution |

## Plotting

Requires Python 3 with matplotlib, pandas, and numpy:

```bash
# Plot the full suite (sine, constant, linear ramp with CoDel internals):
python3 plot_suite.py ~/snake-load-test-charts/<timestamp>

# Plot a linear ramp run:
python3 plot_linear_ramp.py ~/snake-load-test-charts/<timestamp>/tsv/
```

## Easing comparison

Compares CoDel ease-out behavior across log bases (how aggressively count
decays when the queue drains: `count -= floor(log_base(count)/base)`; a larger
base = gentler ease-out). Config-driven via `--config`: the script runs every
(easing × workload) combination, then plots one comparison figure per workload.

```bash
python3 plot_easing_comparison.py --config easing_config.example.json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | (required) | JSON file describing `easings` + `workloads` (see below) |
| `--bench-go` | `bench_suite.go` | Path to bench_suite.go |
| `--jobs` / `-j` | 0 (unlimited) | Max concurrent benchmark processes; use `1` for serial runs to minimize contention noise |

### Config format

Each `easings` entry is a bare number (the log base) or `{"base": N}`. Any
workload field omitted falls back to the defaults. An optional top-level
`"compare"` is `"easing"` (default — one figure per workload, columns = bases)
or `"workload"` (one figure per profile, columns = workloads, single easing).
See `easing_config.example.json`.

```json
{
  "easings": [2, {"base": 2.5}, 3],
  "workloads": [
    {"label": "ramp", "profile": "linear_ramp", "peak": 80, "duration_ms": 20000,
     "work_ms": 2, "target_ms": 5, "interval_ms": 100},
    {"label": "sine", "profile": "sine", "peak": 100, "period_ms": 1000}
  ]
}
```
