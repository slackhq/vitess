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

To compare multiple exponent values side-by-side:

```bash
for exp in 0.25 0.5 0.75 1.0 1.5 2.0; do
  go run bench_suite.go --exponent $exp --out ~/snake-load-test-charts/tsv/easing-comparison/easing_${exp}
done

python3 plot_easing_comparison.py
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--out` | auto | Output directory for TSVs |
| `--exponent` | 1.0 | CoDel control law exponent |
| `--target-ms` | 5 | CoDel target delay (ms) |
| `--interval-ms` | 100 | CoDel observation interval (ms) |

Defaults match production vttablet values (`--loadshed-exponent`, `--loadshed-target`,
`--loadshed-interval`). Override to experiment with alternative tuning.
