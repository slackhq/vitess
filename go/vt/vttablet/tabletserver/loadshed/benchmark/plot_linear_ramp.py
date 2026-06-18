#!/usr/bin/env python3
"""Plot linear ramp test results from snake-22 with Stats() data (7 rows).

Usage:
    python plot_linear_ramp.py [TSV_DIR]

TSV_DIR defaults to the most recent run under ~/snake-load-test-charts/tsv/.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from pathlib import Path
from datetime import datetime

BASE = Path.home() / "snake-load-test-charts"
TSV_BASE = BASE / "tsv"

def find_latest_run():
    """Find the most recent timestamped run dir under tsv/<date>/."""
    date_dirs = sorted(TSV_BASE.iterdir(), reverse=True)
    for d in date_dirs:
        if not d.is_dir() or d.name == "easing-comparison":
            continue
        runs = sorted(d.iterdir(), reverse=True)
        for r in runs:
            if r.is_dir():
                return r
    return None

if len(sys.argv) > 1:
    RUN_DIR = sys.argv[1]
else:
    latest = find_latest_run()
    if latest is None:
        print("ERROR: no run dirs found under", TSV_BASE)
        sys.exit(1)
    RUN_DIR = str(latest)

KEY = "linear_ramp__0_to_80x_cap__work_half_target"
DUR_MS = 20000
NUM_ROWS = 7

# Config embedded for chart verification
CAPACITY = 10
WORK_MS = 2
TARGET_MS = 5
INTERVAL_MS = 100
EXPONENT = 1.0
PEAK_MULT = 80


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


tsv_dir = Path(RUN_DIR)
events_file = tsv_dir / f"{KEY}.tsv"
stats_file = tsv_dir / f"{KEY}_stats.tsv"

if not events_file.exists():
    print(f"ERROR: {events_file} not found")
    sys.exit(1)

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

fig, axes = plt.subplots(NUM_ROWS, 1, figsize=(12, 24), squeeze=True)
fig.suptitle(
    f"Snake (bwines/snake-22) — Linear Ramp 0→{PEAK_MULT}× capacity\n"
    f"capacity={CAPACITY}, work={WORK_MS}ms, target={TARGET_MS}ms, "
    f"interval={INTERVAL_MS}ms, exponent={EXPONENT} (no easing)",
    fontsize=12, y=0.995,
)

# Row 0: Grant rate
ax = axes[0]
ax.plot(t_granted, hz_granted, color="green", linewidth=1.3)
ax.fill_between(t_granted, hz_granted, alpha=0.15, color="green")
ax.set_xlim(0, xlim)
ax.set_ylabel("Hz")
ax.set_title("Grant Rate", fontsize=10, fontweight="bold")
ax.yaxis.set_major_locator(mticker.MaxNLocator(5))
ax.grid(True, alpha=0.3)
ax.ticklabel_format(useOffset=False, style='plain', axis='y')

# Row 1: Issued / Granted / Shed Hz
ax = axes[1]
ax.fill_between(t_issued, hz_issued, alpha=0.12, color="grey")
ax.plot(t_issued, hz_issued, color="grey", linewidth=0.8, label="issued")
ax.plot(t_granted, hz_granted, color="green", linewidth=1.3, label="granted")
ax.plot(t_shed, hz_shed, color="red", linewidth=1, label="shed")
ax.text(0.97, 0.95, f"shed: {shed_pct:.1f}%",
        transform=ax.transAxes, ha="right", va="top", fontsize=9,
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.8))
ax.set_xlim(0, xlim)
ax.set_ylabel("Hz")
ax.set_title("Issued / Granted / Shed", fontsize=10, fontweight="bold")
ax.yaxis.set_major_locator(mticker.MaxNLocator(5))
ax.grid(True, alpha=0.3)
ax.ticklabel_format(useOffset=False, style='plain', axis='y')
ax.legend(loc="upper left", fontsize=8)

# Stats panels (rows 2-5)
has_stats = stats_file.exists()
if has_stats:
    stats_df = pd.read_csv(stats_file, sep="\t")
    stats_df = stats_df[stats_df["ts_ms"] <= DUR_MS]
    t = stats_df["ts_ms"].values / 1000

    # Row 2: Queue depth
    ax = axes[2]
    ax.plot(t, stats_df["queue_len"].values, color="blue", linewidth=1, label="queue total")
    ax.plot(t, stats_df["droppable_len"].values, color="orange", linewidth=1, label="droppable")
    ax.plot(t, stats_df["holder_count"].values, color="green", linewidth=1, label="holders")
    ax.set_xlim(0, xlim)
    ax.set_ylabel("count")
    ax.set_title("Queue Depth", fontsize=10, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    ax.legend(fontsize=7, loc="upper left")

    # Row 3: CoDel dropping state + drop count
    ax = axes[3]
    dropping = stats_df["dropping"].values.astype(float)
    max_count = stats_df["drop_count"].max() if stats_df["drop_count"].max() > 0 else 1
    ax.fill_between(t, 0, dropping * max_count, alpha=0.15, color="red", label="dropping state")
    ax.plot(t, stats_df["drop_count"].values, color="purple", linewidth=1, label="CoDel count")
    ax.set_xlim(0, xlim)
    ax.set_ylabel("count")
    ax.set_title("CoDel State (shaded = dropping)", fontsize=10, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    ax.legend(fontsize=7, loc="upper left")

    # Row 4: Current interval (log scale)
    ax = axes[4]
    interval_ms = stats_df["current_interval_ns"].values / 1_000_000
    ax.plot(t, interval_ms, color="darkblue", linewidth=1)
    ax.set_yscale("log")
    ax.set_xlim(0, xlim)
    ax.set_ylabel("interval (ms)")
    ax.set_title("CoDel Current Interval (log)", fontsize=10, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.axhline(y=INTERVAL_MS, color="grey", linestyle="--", linewidth=0.7, alpha=0.5)

    # Row 5: Cumulative unfilled slots
    ax = axes[5]
    holders = stats_df["holder_count"].values
    unfilled = np.maximum(CAPACITY - holders, 0)
    cumulative_unfilled = np.cumsum(unfilled)
    ax.plot(t, cumulative_unfilled, color="darkorange", linewidth=1.2)
    ax.fill_between(t, cumulative_unfilled, alpha=0.15, color="orange")
    ax.set_xlim(0, xlim)
    ax.set_ylabel("cumulative slots")
    ax.set_title("Cumulative Unfilled Slots", fontsize=10, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')
else:
    for row in range(2, 6):
        axes[row].text(0.5, 0.5, "no stats data", transform=axes[row].transAxes, ha="center", fontsize=12)
        axes[row].set_xlim(0, xlim)

# Row 6: Request latency (p50, p95, p99)
ax = axes[6]
t_lat, p50, p95, p99 = compute_latency_percentiles(df, bin_ms, max_ts)
if t_lat is not None:
    ax.plot(t_lat, p50, color="green", linewidth=1, label="p50")
    ax.plot(t_lat, p95, color="orange", linewidth=1, label="p95")
    ax.plot(t_lat, p99, color="red", linewidth=1, label="p99")
ax.set_xlim(0, xlim)
ax.set_ylim(bottom=0)
ax.set_ylabel("ms")
ax.set_xlabel("time (s)")
ax.set_title("Request Latency (granted only)", fontsize=10, fontweight="bold")
ax.grid(True, alpha=0.3)
ax.ticklabel_format(useOffset=False, style='plain', axis='y')
ax.legend(fontsize=7, loc="upper left")

plt.tight_layout(rect=[0, 0, 1, 0.97])

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
out_path = str(BASE / f"{timestamp}_snake22_linear_ramp.png")
plt.savefig(out_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved: {out_path}")
