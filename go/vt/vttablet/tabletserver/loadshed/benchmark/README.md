# Snake Benchmark Suite

Exercises Snake under synthetic load to visualize CoDel behavior (grant rate, shedding,
queue depth, interval adaptation, latency). Outputs TSV files consumed by the plotting
scripts.

## Running the bench

```bash
cd go/vt/vttablet/tabletserver/loadshed/benchmark

# With production defaults (exponent=1.0, target=5ms, interval=100ms):
go run bench_suite.go

# Override CoDel parameters:
go run bench_suite.go --exponent 0.5 --target-ms 10 --interval-ms 200

# Custom output directory:
go run bench_suite.go --out /tmp/my-run
```

Output lands in `~/snake-load-test-charts/tsv/<date>/<timestamp>/` by default.

## Plotting

Requires Python 3 with matplotlib and pandas:

```bash
# Plot the most recent run:
python3 plot_linear_ramp.py

# Plot a specific run:
python3 plot_linear_ramp.py ~/snake-load-test-charts/tsv/2026-06-17/2026-06-17_14-30-00/
```

Output PNG goes to `~/snake-load-test-charts/`.

## Easing comparison

Compares CoDel ease-out behavior across divisor values (how aggressively count
decays when the queue drains). Runs all 6 divisors in parallel, then plots:

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
| `--bench-go` | `../snakeserver/bench_suite.go` | Path to bench_suite.go with `-easing` flag |
| `--filter` | `linear_ramp__0_to_80x_cap__work_half_target` | Only run tests matching this label |

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--out` | auto | Output directory for TSVs |
| `--exponent` | 1.0 | CoDel control law exponent |
| `--target-ms` | 5 | CoDel target delay (ms) |
| `--interval-ms` | 100 | CoDel observation interval (ms) |

Defaults match production vttablet values (`--loadshed-exponent`, `--loadshed-target`,
`--loadshed-interval`). Override to experiment with alternative tuning.
