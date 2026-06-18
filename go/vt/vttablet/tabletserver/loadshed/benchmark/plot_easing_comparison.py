#!/usr/bin/env python3
"""Plot 6-column comparison of easing divisor values (work=2ms).

Optionally runs the Go bench suite in parallel before plotting (--run).
"""

import argparse
import os
import subprocess
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from pathlib import Path
from datetime import datetime

BASE = Path.home() / "snake-load-test-charts"
EASING_BASE = BASE / "tsv" / "easing-comparison"

DIVISORS = sorted([1.0, 2**(1/4), 2**(1/3), 2**(1/2), 2.0, 4.0, 8.0, 16.0])


def divisor_label(div):
    """Human-readable label for a divisor value."""
    import math
    if div == 1.0:
        return "1 (no decay)"
    for denom in [2, 3, 4]:
        if abs(div - 2**(1/denom)) < 1e-9:
            return f"2^(1/{denom}) ≈ {div:.3f}"
    if div == int(div):
        return str(int(div))
    return f"{div:.3f}"


def divisor_dirname(div):
    """Stable directory name for a divisor value."""
    import math
    for denom in [2, 3, 4]:
        if abs(div - 2**(1/denom)) < 1e-9:
            return f"easing_2pow1over{denom}"
    if div == int(div):
        return f"easing_{int(div)}"
    return f"easing_{div:.4f}"


KEY = "linear_ramp__0_to_80x_cap__work_half_target"
DUR_MS = 20000
NUM_ROWS = 7  # grant rate, issued/shed Hz, queue depth, codel state, interval, cumulative unfilled, latency


def run_benchmarks(bench_go_path: str, filter_label: str):
    """Run all easing divisor benchmarks in parallel."""
    procs = []
    for div in DIVISORS:
        out_dir = str(EASING_BASE / divisor_dirname(div))
        os.makedirs(out_dir, exist_ok=True)
        cmd = [
            "go", "run", bench_go_path,
            "-easing", str(div),
            "-filter", filter_label,
            "-out", out_dir,
        ]
        print(f"  Starting easing={divisor_label(div)} ...")
        procs.append((div, subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)))

    for div, proc in procs:
        stdout, _ = proc.communicate()
        status = "OK" if proc.returncode == 0 else f"FAILED (rc={proc.returncode})"
        print(f"  easing={divisor_label(div)}: {status}")
        if proc.returncode != 0:
            print(stdout.decode(), file=sys.stderr)


def parse_args():
    parser = argparse.ArgumentParser(description="Plot easing divisor comparison.")
    parser.add_argument("--run", action="store_true",
                        help="Run Go bench suite for all divisors in parallel before plotting")
    parser.add_argument("--bench-go", type=str,
                        default=str(Path(__file__).parent.parent / "snakeserver" / "bench_suite.go"),
                        help="Path to bench_suite.go with -easing flag")
    parser.add_argument("--filter", type=str, default=KEY,
                        help="Only run tests whose label contains this substring")
    return parser.parse_args()


args = parse_args()

if args.run:
    print(f"Running {len(DIVISORS)} easing benchmarks in parallel...")
    run_benchmarks(args.bench_go, args.filter)
    print()

BASE_BRANCH_RUN = str(Path.home() / "snake-load-test-charts/tsv/2026-06-18/2026-06-18_11-26-05")
EASING_RUNS = [
    (None, BASE_BRANCH_RUN),  # base branch, no easing machinery at all
] + [(div, str(EASING_BASE / divisor_dirname(div))) for div in DIVISORS]
NUM_COLS = len(EASING_RUNS)


def compute_hz_series(df, event_type, bin_ms, max_ts):
    subset = df[df["event"] == event_type]["ts_ms"].values
    bins = np.arange(0, max_ts + bin_ms, bin_ms)
    if len(subset) == 0:
        centers = (bins[:-1] + bins[1:]) / 2 / 1000
        return centers, np.zeros(len(centers))
    counts, edges = np.histogram(subset, bins=bins)
    centers = (edges[:-1] + edges[1:]) / 2 / 1000
    hz = counts / (bin_ms / 1000)
    return centers, hz


def compute_latency_percentiles(df, bin_ms, max_ts):
    granted = df[df["event"] == "granted"].copy()
    if len(granted) == 0 or "latency_ms" not in granted.columns:
        return None, None, None, None
    granted = granted.dropna(subset=["latency_ms"])
    granted["latency_ms"] = pd.to_numeric(granted["latency_ms"], errors="coerce")
    granted = granted.dropna(subset=["latency_ms"])
    if len(granted) == 0:
        return None, None, None, None

    bins = np.arange(0, max_ts + bin_ms, bin_ms)
    centers = (bins[:-1] + bins[1:]) / 2 / 1000
    granted["bin"] = pd.cut(granted["ts_ms"], bins=bins, labels=False)

    p50 = np.full(len(centers), np.nan)
    p95 = np.full(len(centers), np.nan)
    p99 = np.full(len(centers), np.nan)

    for i, grp in granted.groupby("bin")["latency_ms"]:
        if len(grp) >= 3:
            idx = int(i)
            if 0 <= idx < len(centers):
                p50[idx] = grp.quantile(0.5)
                p95[idx] = grp.quantile(0.95)
                p99[idx] = grp.quantile(0.99)

    return centers, p50, p95, p99


fig, axes = plt.subplots(NUM_ROWS, NUM_COLS, figsize=(45, 22), squeeze=False)
fig.suptitle(
    "CoDel Easing Divisor Comparison — Linear Ramp 0→80× capacity, work=2ms\n"
    "capacity=10, target=5ms, interval=100ms, exponent=1",
    fontsize=13, y=0.99,
)

for col_idx, (divisor, run_dir) in enumerate(EASING_RUNS):
    # Base branch data is directly in the dir; easing runs use tsv/ subdir.
    tsv_dir = Path(run_dir) / "tsv" if divisor is not None else Path(run_dir)
    if not tsv_dir.exists():
        tsv_dir = Path(run_dir)
    events_file = tsv_dir / f"{KEY}.tsv"
    stats_file = tsv_dir / f"{KEY}_stats.tsv"
    col_label = "base branch\n(no easing)" if divisor is None else f"divisor = {divisor_label(divisor)}"

    if not events_file.exists():
        for row in range(NUM_ROWS):
            axes[row][col_idx].text(0.5, 0.5, "no data", transform=axes[row][col_idx].transAxes, ha="center")
        continue

    df = pd.read_csv(events_file, sep="\t")
    max_ts = min(df["ts_ms"].max(), DUR_MS + 500)
    bin_ms = max(5, int(max_ts / 150))

    t_issued, hz_issued = compute_hz_series(df, "issued", bin_ms, max_ts)
    t_granted, hz_granted = compute_hz_series(df, "granted", bin_ms, max_ts)
    t_shed, hz_shed = compute_hz_series(df, "shed", bin_ms, max_ts)

    xlim = DUR_MS / 1000 * 1.02

    total_issued = len(df[df["event"] == "issued"])
    total_shed = len(df[df["event"] == "shed"])
    shed_pct = total_shed / total_issued * 100 if total_issued > 0 else 0

    # Row 0: Grant rate
    ax = axes[0][col_idx]
    ax.plot(t_granted, hz_granted, color="green", linewidth=1.3)
    ax.fill_between(t_granted, hz_granted, alpha=0.15, color="green")
    ax.set_title(col_label, fontsize=11, fontweight="bold")
    ax.set_xlim(0, xlim)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(5))
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    if col_idx == 0:
        ax.set_ylabel("Hz")
        ax.set_title("Grant Rate\n" + col_label, fontsize=10, fontweight="bold")

    # Row 1: Hz chart (issued + shed)
    ax = axes[1][col_idx]
    ax.fill_between(t_issued, hz_issued, alpha=0.12, color="grey")
    ax.plot(t_issued, hz_issued, color="grey", linewidth=0.8, label="issued")
    ax.plot(t_granted, hz_granted, color="green", linewidth=1.3, label="granted")
    ax.plot(t_shed, hz_shed, color="red", linewidth=1, label="shed")
    ax.text(0.97, 0.95, f"shed: {shed_pct:.1f}%",
            transform=ax.transAxes, ha="right", va="top", fontsize=9,
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.8))
    ax.set_xlim(0, xlim)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(5))
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    if col_idx == 0:
        ax.legend(loc="upper left", fontsize=8)
        ax.set_ylabel("Hz")

    # Rows 2-5: Stats panels
    if stats_file.exists():
        stats_df = pd.read_csv(stats_file, sep="\t")
        stats_df = stats_df[stats_df["ts_ms"] <= DUR_MS]
        t = stats_df["ts_ms"].values / 1000

        # Row 2: Queue depth
        ax = axes[2][col_idx]
        ax.plot(t, stats_df["queue_len"].values, color="blue", linewidth=1, label="queue total")
        ax.plot(t, stats_df["droppable_len"].values, color="orange", linewidth=1, label="droppable")
        ax.plot(t, stats_df["holder_count"].values, color="green", linewidth=1, label="holders")
        ax.set_xlim(0, xlim)
        ax.grid(True, alpha=0.3)
        ax.ticklabel_format(useOffset=False, style='plain', axis='y')
        if col_idx == 0:
            ax.set_ylabel("count")
            ax.legend(fontsize=7, loc="upper left")
            ax.set_title("Queue Depth", fontsize=9)

        # Row 3: CoDel dropping state + drop count
        ax = axes[3][col_idx]
        dropping = stats_df["dropping"].values.astype(float)
        max_count = stats_df["drop_count"].max() if stats_df["drop_count"].max() > 0 else 1
        ax.fill_between(t, 0, dropping * max_count, alpha=0.15, color="red", label="dropping state")
        ax.plot(t, stats_df["drop_count"].values, color="purple", linewidth=1, label="CoDel count")
        ax.set_xlim(0, xlim)
        ax.grid(True, alpha=0.3)
        ax.ticklabel_format(useOffset=False, style='plain', axis='y')
        if col_idx == 0:
            ax.set_ylabel("count")
            ax.legend(fontsize=7, loc="upper left")
            ax.set_title("CoDel State (shaded = dropping)", fontsize=9)

        # Row 4: Current interval (log scale)
        ax = axes[4][col_idx]
        interval_ms = stats_df["current_interval_ns"].values / 1_000_000
        ax.plot(t, interval_ms, color="darkblue", linewidth=1)
        ax.set_yscale("log")
        ax.set_xlim(0, xlim)
        ax.grid(True, alpha=0.3)
        ax.axhline(y=100, color="grey", linestyle="--", linewidth=0.7, alpha=0.5)
        if col_idx == 0:
            ax.set_ylabel("interval (ms)")
            ax.set_title("CoDel Current Interval (log)", fontsize=9)

        # Row 5: Cumulative unfilled slots
        ax = axes[5][col_idx]
        capacity = 10
        holders = stats_df["holder_count"].values
        unfilled = np.maximum(capacity - holders, 0)
        cumulative_unfilled = np.cumsum(unfilled)
        ax.plot(t, cumulative_unfilled, color="darkorange", linewidth=1.2)
        ax.fill_between(t, cumulative_unfilled, alpha=0.15, color="orange")
        ax.set_xlim(0, xlim)
        ax.set_ylim(0, 2000)
        ax.grid(True, alpha=0.3)
        ax.ticklabel_format(useOffset=False, style='plain', axis='y')
        if col_idx == 0:
            ax.set_ylabel("cumulative slots")
            ax.set_title("Cumulative Unfilled Slots", fontsize=9)

    # Row 6: Request latency (p50, p95, p99)
    ax = axes[6][col_idx]
    t_lat, p50, p95, p99 = compute_latency_percentiles(df, bin_ms, max_ts)
    if t_lat is not None:
        ax.plot(t_lat, p50, color="green", linewidth=1, label="p50")
        ax.plot(t_lat, p95, color="orange", linewidth=1, label="p95")
        ax.plot(t_lat, p99, color="red", linewidth=1, label="p99")
    ax.set_xlim(0, xlim)
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    if col_idx == 0:
        ax.set_ylabel("ms")
        ax.legend(fontsize=7, loc="upper left")
        ax.set_title("Request Latency (granted only)", fontsize=9)

# X labels on bottom row
for col_idx in range(NUM_COLS):
    axes[NUM_ROWS - 1][col_idx].set_xlabel("time (s)")

plt.tight_layout(rect=[0, 0, 1, 0.96])
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
out_path = str(BASE / f"{timestamp}_easing_divisor_comparison.png")
plt.savefig(out_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved: {out_path}")
