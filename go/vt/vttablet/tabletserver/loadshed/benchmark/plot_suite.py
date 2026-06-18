#!/usr/bin/env python3
"""Plot snake bench suite results as grids of Hz time-series charts."""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path

INPUT_DIR = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/snake-load-test-charts")
OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else INPUT_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Remove any previously-generated PNGs so stale charts from prior runs
# (e.g., empty sine/constant charts when only linear_ramp data exists)
# don't persist and confuse users.
for old_png in Path(OUTPUT_DIR).glob("snake_*.png"):
    old_png.unlink()



def compute_hz_series(df, event_type, bin_ms, max_ts):
    """Compute Hz (events per second) in bins."""
    subset = df[df["event"] == event_type]["ts_ms"].values
    bins = np.arange(0, max_ts + bin_ms, bin_ms)
    if len(subset) == 0:
        centers = (bins[:-1] + bins[1:]) / 2 / 1000
        return centers, np.zeros(len(centers))
    counts, edges = np.histogram(subset, bins=bins)
    centers = (edges[:-1] + edges[1:]) / 2 / 1000
    hz = counts / (bin_ms / 1000)
    return centers, hz


def plot_single(ax, df, duration_ms=None, dual_yaxis=False):
    """Plot issued/granted/shed Hz on a single axis (or dual y-axis)."""
    max_ts = df["ts_ms"].max() if len(df) > 0 else 1
    if duration_ms is not None:
        max_ts = min(max_ts, duration_ms + 500)
    bin_ms = max(5, int(max_ts / 150))

    t_issued, hz_issued = compute_hz_series(df, "issued", bin_ms, max_ts)
    t_granted, hz_granted = compute_hz_series(df, "granted", bin_ms, max_ts)
    t_shed, hz_shed = compute_hz_series(df, "shed", bin_ms, max_ts)

    if dual_yaxis:
        ax.plot(t_granted, hz_granted, color="green", linewidth=1.3, label="granted")
        ax.plot(t_shed, hz_shed, color="red", linewidth=1, label="shed")
        ax.set_ylabel("granted/shed Hz", fontsize=8)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(5))

        ax2 = ax.twinx()
        ax2.fill_between(t_issued, hz_issued, alpha=0.10, color="grey")
        ax2.plot(t_issued, hz_issued, color="grey", linewidth=0.8, label="issued")
        ax2.set_ylabel("issued Hz", fontsize=8, color="grey")
        ax2.tick_params(axis="y", labelcolor="grey", labelsize=7)
        ax2.yaxis.set_major_locator(mticker.MaxNLocator(4))
    else:
        ax.fill_between(t_issued, hz_issued, alpha=0.12, color="grey")
        ax.plot(t_issued, hz_issued, color="grey", linewidth=0.8, label="issued")
        ax.plot(t_granted, hz_granted, color="green", linewidth=1.3, label="granted")
        ax.plot(t_shed, hz_shed, color="red", linewidth=1, label="shed")

    total_issued = len(df[df["event"] == "issued"])
    total_shed = len(df[df["event"] == "shed"])
    shed_pct = total_shed / total_issued * 100 if total_issued > 0 else 0
    ax.text(0.97, 0.95, f"shed: {shed_pct:.1f}%",
            transform=ax.transAxes, ha="right", va="top", fontsize=9,
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.8))

    if duration_ms is not None:
        ax.set_xlim(0, duration_ms / 1000 * 1.02)

    ax.yaxis.set_major_locator(mticker.MaxNLocator(5))
    ax.grid(True, alpha=0.3)


def plot_stats_panels(axes_row, stats_df, duration_ms):
    """Plot CoDel internals on a row of 3 axes: queue depth, dropping state, current interval."""
    t = stats_df["ts_ms"].values / 1000  # seconds
    xlim = duration_ms / 1000 * 1.02

    # Panel 1: Queue depth (total, droppable, holders)
    ax = axes_row[0]
    ax.plot(t, stats_df["queue_len"].values, color="blue", linewidth=1, label="queue total")
    ax.plot(t, stats_df["droppable_len"].values, color="orange", linewidth=1, label="droppable")
    ax.plot(t, stats_df["holder_count"].values, color="green", linewidth=1, label="holders")
    ax.set_ylabel("count")
    ax.set_title("Queue Depth", fontsize=9)
    ax.legend(fontsize=7, loc="upper left")
    ax.set_xlim(0, xlim)
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')

    # Panel 2: CoDel dropping state + drop count
    ax = axes_row[1]
    dropping = stats_df["dropping"].values.astype(float)
    ax.fill_between(t, 0, dropping * stats_df["drop_count"].max() if stats_df["drop_count"].max() > 0 else 1,
                    alpha=0.15, color="red", label="dropping state")
    ax.plot(t, stats_df["drop_count"].values, color="purple", linewidth=1, label="drop count")
    ax.set_ylabel("drop count")
    ax.set_title("CoDel State (shaded = dropping)", fontsize=9)
    ax.legend(fontsize=7, loc="upper left")
    ax.set_xlim(0, xlim)
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')

    # Panel 3: Current interval (the control law output)
    ax = axes_row[2]
    interval_ms = stats_df["current_interval_ns"].values / 1_000_000  # ns -> ms
    ax.plot(t, interval_ms, color="darkblue", linewidth=1)
    ax.set_ylabel("interval (ms)")
    ax.set_title("CoDel Current Interval", fontsize=9)
    ax.set_xlim(0, xlim)
    ax.grid(True, alpha=0.3)
    ax.axhline(y=100, color="grey", linestyle="--", linewidth=0.7, alpha=0.5)
    ax.text(xlim * 0.98, 100, "configured interval", ha="right", va="bottom", fontsize=7, color="grey")
    ax.ticklabel_format(useOffset=False, style='plain', axis='y')


# Parse all TSV files (look in tsv/ subdir first, fall back to top-level)
tsv_dir = Path(INPUT_DIR) / "tsv"
if not tsv_dir.exists():
    tsv_dir = Path(INPUT_DIR)

all_files = {}
for f in sorted(tsv_dir.glob("*.tsv")):
    if "_stats" not in f.stem:
        all_files[f.stem] = f

stats_files = {}
for f in sorted(tsv_dir.glob("*_stats.tsv")):
    key = f.stem.replace("_stats", "")
    stats_files[key] = f

# --- Sine wave grid: one figure per work duration ---
period_order = ["2x_interval", "10x_interval", "20x_interval"]
peak_order = ["20x_cap", "100x_cap", "300x_cap"]
work_order = ["half_target", "equal_target"]

for work in work_order:
    # Check if any sine data exists for this work duration
    has_sine_data = any(
        f"sine__period_{period}__peak_{peak}__work_{work}" in all_files
        for period in period_order
        for peak in peak_order
    )
    if not has_sine_data:
        continue

    fig, axes = plt.subplots(
        len(peak_order), len(period_order),
        figsize=(16, 12), squeeze=False, sharex="col"
    )

    work_ms = "2ms" if work == "half_target" else "5ms"
    fig.suptitle(
        f"Snake Sine Wave Bench: work={work_ms}, capacity=10, target=5ms, interval=100ms\n"
        f"Left axis: granted (green) + shed (red) Hz | Right axis: issued (grey) Hz",
        fontsize=13, y=0.98
    )

    for col_idx, period in enumerate(period_order):
        for row_idx, peak in enumerate(peak_order):
            ax = axes[row_idx][col_idx]

            key = f"sine__period_{period}__peak_{peak}__work_{work}"
            if key not in all_files:
                ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center")
                continue

            df = pd.read_csv(all_files[key], sep="\t")
            period_ms_val = {"2x_interval": 200, "10x_interval": 1000, "20x_interval": 2000}[period]
            plot_single(ax, df, duration_ms=period_ms_val * 2, dual_yaxis=True)

            if row_idx == 0:
                period_ms = {"2x_interval": "200ms", "10x_interval": "1000ms", "20x_interval": "2000ms"}[period]
                ax.set_title(f"period={period_ms}", fontsize=10)
            if col_idx == 0:
                peak_val = {"20x_cap": "20x", "100x_cap": "100x", "300x_cap": "300x"}[peak]
                ax.set_ylabel(f"peak={peak_val}\ngranted/shed Hz", fontsize=9)
            if row_idx == len(peak_order) - 1:
                ax.set_xlabel("time (s)")
            if row_idx == 0 and col_idx == 0:
                ax.legend(loc="upper left", fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    out_path = os.path.join(OUTPUT_DIR, f"snake_sine_work_{work}.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")

# --- Constant + Linear Ramp: with CoDel internals ---
# Layout: 4 rows x 2 cols per profile+work combo
# Row 1: Hz chart, Row 2-4: queue depth, dropping state, interval
# Two figures: one per profile, each with 2 work durations stacked

profile_configs = [
    ("constant__5x_cap", "Constant 5x capacity", 20000),
    ("linear_ramp__0_to_80x_cap", "Linear ramp 0->80x capacity", 20000),
]

for prefix, title, dur_ms in profile_configs:
    # Check if any data exists for this profile
    has_profile_data = any(
        f"{prefix}__work_{work}" in all_files
        for work in work_order
    )
    if not has_profile_data:
        continue

    fig, axes = plt.subplots(4, 2, figsize=(14, 14), squeeze=False)
    fig.suptitle(
        f"Snake Bench: {title} (capacity=10, target=5ms, interval=100ms)\n"
        f"Top row: throughput Hz | Rows 2-4: CoDel internals",
        fontsize=12, y=0.99
    )

    for col_idx, work in enumerate(work_order):
        key = f"{prefix}__work_{work}"
        work_ms = "2ms" if work == "half_target" else "5ms"

        # Row 0: Hz chart
        ax = axes[0][col_idx]
        if key in all_files:
            df = pd.read_csv(all_files[key], sep="\t")
            plot_single(ax, df, duration_ms=dur_ms)
        ax.set_title(f"work={work_ms}", fontsize=11)
        ax.ticklabel_format(useOffset=False, style='plain', axis='y')
        if col_idx == 0:
            ax.legend(loc="upper left", fontsize=8)

        # Rows 1-3: Stats panels
        if key in stats_files:
            stats_df = pd.read_csv(stats_files[key], sep="\t")
            stats_df = stats_df[stats_df["ts_ms"] <= dur_ms]
            plot_stats_panels([axes[1][col_idx], axes[2][col_idx], axes[3][col_idx]], stats_df, dur_ms)

    # X labels on bottom row only
    for col_idx in range(2):
        axes[3][col_idx].set_xlabel("time (s)")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    out_path = os.path.join(OUTPUT_DIR, f"snake_{prefix.split('__')[0]}_with_codel.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")

print("Done.")
