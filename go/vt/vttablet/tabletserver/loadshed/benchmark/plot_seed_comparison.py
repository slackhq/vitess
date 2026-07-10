#!/usr/bin/env python3
"""Plot an N-column CoDel comparison across brown_noise seeds (columns vary by
seed/workload), matching the 7-row layout: Grant Rate, Issued/Granted/Shed,
Queue Depth, CoDel State, CoDel Current Interval (log), Cumulative Unfilled
Slots, and Request Latency (p50/p95/p99, clamped).

Usage:
  plot_seed_comparison.py <tsv_dir> --labels s1_t5,s2_t5,... [--bin-ms 200]
    [--capacity 10] [--lat-clip-ms 500] [--title "..."] [--out path.png]
"""
import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

p = argparse.ArgumentParser()
p.add_argument("tsv_dir")
p.add_argument("--labels", required=True, help="comma-separated TSV labels, one per column")
p.add_argument("--bin-ms", type=int, default=200)
p.add_argument("--capacity", type=int, default=10)
p.add_argument("--lat-clip-ms", type=float, default=500)
p.add_argument("--title", default="CoDel Comparison — brown_noise (columns vary by workload)")
p.add_argument("--subtitle", default="")
p.add_argument("--out", default="")
args = p.parse_args()

tsv_dir = Path(args.tsv_dir)
labels = [s for s in args.labels.split(",") if s]
BIN_MS = args.bin_ms
LAT_CLIP = args.lat_clip_ms


def hz(df, event, bins):
    v = df[df["event"] == event]["ts_ms"].values
    counts, edges = np.histogram(v, bins=bins)
    centers = (edges[:-1] + edges[1:]) / 2 / 1000
    return centers, counts / (BIN_MS / 1000)


def latency_pctiles(df, bins):
    g = df[df["event"] == "granted"].copy()
    centers = (bins[:-1] + bins[1:]) / 2 / 1000
    p50 = np.full(len(centers), np.nan)
    p95 = np.full(len(centers), np.nan)
    p99 = np.full(len(centers), np.nan)
    if len(g) == 0:
        return centers, p50, p95, p99
    g["latency_ms"] = pd.to_numeric(g["latency_ms"], errors="coerce")
    g = g.dropna(subset=["latency_ms"])
    g["bin"] = pd.cut(g["ts_ms"], bins=bins, labels=False)
    for i, grp in g.groupby("bin")["latency_ms"]:
        if len(grp) >= 3:
            idx = int(i)
            if 0 <= idx < len(centers):
                p50[idx] = grp.quantile(0.5)
                p95[idx] = grp.quantile(0.95)
                p99[idx] = grp.quantile(0.99)
    return centers, p50, p95, p99


ncols = len(labels)
NROWS = 7
fig, axes = plt.subplots(NROWS, ncols, figsize=(3.2 * ncols, 2.3 * NROWS), squeeze=False)
row_titles = [
    "Grant Rate",
    "Issued / Granted / Shed",
    "Queue Depth",
    "CoDel State (shaded = dropping)",
    "CoDel Current Interval (log)",
    "Cumulative Unfilled Slots",
    f"Request Latency (granted only, ≤{int(LAT_CLIP)}ms)",
]

for col, label in enumerate(labels):
    ev = pd.read_csv(tsv_dir / f"{label}.tsv", sep="\t")
    st = pd.read_csv(tsv_dir / f"{label}_stats.tsv", sep="\t")
    max_ts = max(ev["ts_ms"].max(), st["ts_ms"].max())
    bins = np.arange(0, max_ts + BIN_MS, BIN_MS)
    t_st = st["ts_ms"].values / 1000
    xlim = max_ts / 1000

    # Row 0: grant rate
    ax = axes[0][col]
    tg, hg = hz(ev, "granted", bins)
    ax.fill_between(tg, 0, hg, color="green", alpha=0.15)
    ax.plot(tg, hg, color="green", linewidth=0.8)
    if col == 0:
        ax.set_ylabel("Hz", fontsize=7)
    ax.set_title(label, fontsize=10, fontweight="bold")

    # Row 1: issued/granted/shed
    ax = axes[1][col]
    ti, hi = hz(ev, "issued", bins)
    _, hgg = hz(ev, "granted", bins)
    ts_, hs = hz(ev, "shed", bins)
    ax.plot(ti, hi, color="grey", linewidth=0.6, label="issued")
    ax.plot(ti, hgg, color="green", linewidth=0.8, label="granted")
    ax.plot(ts_, hs, color="red", linewidth=0.8, label="shed")
    total_issued = len(ev[ev["event"] == "issued"])
    total_shed = len(ev[ev["event"] == "shed"])
    pct = total_shed / total_issued * 100 if total_issued else 0
    ax.text(0.97, 0.93, f"shed: {pct:.1f}%", transform=ax.transAxes,
            ha="right", va="top", fontsize=6,
            bbox=dict(boxstyle="round", fc="white", ec="grey", alpha=0.8))
    if col == 0:
        ax.set_ylabel("Hz", fontsize=7)
        ax.legend(fontsize=5, loc="upper left")

    # Row 2: queue depth
    ax = axes[2][col]
    ax.plot(t_st, st["queue_len"].values, color="blue", linewidth=0.7, label="queue total")
    ax.plot(t_st, st["droppable_len"].values, color="orange", linewidth=0.7, label="droppable")
    ax.plot(t_st, st["holder_count"].values, color="green", linewidth=0.7, label="holders")
    if col == 0:
        ax.set_ylabel("count", fontsize=7)
        ax.legend(fontsize=5, loc="upper left")

    # Row 3: CoDel state (shaded=dropping) + drop count
    ax = axes[3][col]
    dropping = st["dropping"].values.astype(float)
    dc_max = st["drop_count"].max()
    ax.fill_between(t_st, 0, dropping * (dc_max if dc_max > 0 else 1),
                    alpha=0.15, color="red", label="dropping state")
    ax.plot(t_st, st["drop_count"].values, color="purple", linewidth=0.7, label="CoDel count")
    if col == 0:
        ax.set_ylabel("count", fontsize=7)
        ax.legend(fontsize=5, loc="upper left")

    # Row 4: current interval (log)
    ax = axes[4][col]
    interval_ms = st["current_interval_ns"].values / 1_000_000
    ax.plot(t_st, interval_ms, color="darkblue", linewidth=0.7)
    ax.set_yscale("log")
    if col == 0:
        ax.set_ylabel("interval (ms)", fontsize=7)

    # Row 5: cumulative unfilled slots (capacity - holders integrated over time)
    ax = axes[5][col]
    unfilled = np.maximum(args.capacity - st["holder_count"].values, 0)
    dt = np.diff(t_st, prepend=t_st[0] if len(t_st) else 0)
    cum = np.cumsum(unfilled * dt)
    ax.fill_between(t_st, 0, cum, color="orange", alpha=0.3)
    ax.plot(t_st, cum, color="orange", linewidth=0.7)
    if col == 0:
        ax.set_ylabel("cumulative slots", fontsize=7)

    # Row 6: latency percentiles (clamped)
    ax = axes[6][col]
    tl, p50, p95, p99 = latency_pctiles(ev, bins)
    ax.plot(tl, np.clip(p50, 0, LAT_CLIP), color="green", linewidth=0.7, label="p50")
    ax.plot(tl, np.clip(p95, 0, LAT_CLIP), color="orange", linewidth=0.7, label="p95")
    ax.plot(tl, np.clip(p99, 0, LAT_CLIP), color="red", linewidth=0.7, label="p99")
    ax.set_ylim(0, LAT_CLIP)
    ax.set_xlabel("time (s)", fontsize=7)
    if col == 0:
        ax.set_ylabel(f"ms (clip {int(LAT_CLIP)})", fontsize=7)
        ax.legend(fontsize=5, loc="upper left")

    for r in range(NROWS):
        axes[r][col].set_xlim(0, xlim)
        axes[r][col].grid(True, alpha=0.3)
        axes[r][col].tick_params(labelsize=6)

fig.suptitle(f"{args.title}\n{args.subtitle}", fontsize=12)
plt.tight_layout(rect=[0.02, 0, 1, 0.96])

# Row labels in the left margin, vertically centered on each row (drawn after
# layout so positions are final).
for r in range(NROWS):
    pos = axes[r][0].get_position()
    fig.text(0.005, (pos.y0 + pos.y1) / 2, row_titles[r], rotation=90,
             fontsize=9, fontweight="bold", ha="left", va="center")

out = args.out
if not out:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = str(Path.home() / "snake-load-test-charts" / f"{ts}_seed_comparison.png")
plt.savefig(out, dpi=130, bbox_inches="tight")
plt.close()
print(f"Saved: {out}")
