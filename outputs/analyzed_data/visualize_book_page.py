"""
Visualize book/page pattern pairs and null counts per county.
Produces one polished PNG per county.

Usage:
    python visualize_book_page.py --input nc_records_assessment.jsonl --output-dir outputs
"""

import json
import re
import argparse
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import numpy as np


# ── Fingerprinting ────────────────────────────────────────────────────────────

def fingerprint(s):
    s = str(s).strip()
    result = []
    for ch in s:
        if ch.isupper():   result.append('U')
        elif ch.islower(): result.append('l')
        elif ch.isdigit(): result.append('N')
        else:              result.append(ch)
    return ''.join(result)


def compress_fp(fp):
    return re.sub(r'(.)\1+', lambda m: f'{m.group(1)}({len(m.group())})', fp)


# ── Streaming & Analysis ──────────────────────────────────────────────────────

def stream_records(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def analyze(filepath):
    counties = defaultdict(lambda: {
        "record_count": 0,
        "pair_counter": Counter(),
        "pair_examples": {},
        "null_both": 0,
        "null_book_only": 0,
        "null_page_only": 0,
        "null_neither": 0,
    })

    for record in stream_records(filepath):
        county = record.get("county", "unknown")
        c = counties[county]
        c["record_count"] += 1

        book = record.get("book")
        page = record.get("page")
        book_str = str(book).strip() if book not in (None, "") else None
        page_str = str(page).strip() if page not in (None, "") else None

        if book_str is None and page_str is None:
            c["null_both"] += 1
            continue
        elif book_str is None:
            c["null_book_only"] += 1
        elif page_str is None:
            c["null_page_only"] += 1
        else:
            c["null_neither"] += 1

        book_fp = fingerprint(book_str) if book_str else "NULL"
        page_fp = fingerprint(page_str) if page_str else "NULL"
        pair = (book_fp, page_fp)
        c["pair_counter"][pair] += 1
        c["pair_examples"].setdefault(pair, (book_str or "—", page_str or "—"))

    return counties


# ── Color palette ─────────────────────────────────────────────────────────────

PALETTE = [
    "#FF6B6B", "#FFE66D", "#4ECDC4", "#45B7D1", "#96CEB4",
    "#DDA0DD", "#F0A500", "#7EC8E3", "#FF9A8B", "#A8E6CF",
    "#FF8C94", "#91EAE4", "#FFDAB9", "#B5EAD7", "#C7CEEA",
]

BG        = "#0F1117"
CARD_BG   = "#1A1D27"
TEXT_MAIN = "#F0F0F0"
TEXT_DIM  = "#8B8FA8"
ACCENT    = "#4ECDC4"
GRID_COL  = "#2A2D3A"


# ── Per-county plot ───────────────────────────────────────────────────────────

def plot_county(county, data, output_dir):
    pairs     = data["pair_counter"].most_common()
    examples  = data["pair_examples"]
    total     = data["record_count"]
    n_pairs   = len(pairs)

    # ── Layout ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(14, max(7, 3 + n_pairs * 0.55)), facecolor=BG)
    gs  = gridspec.GridSpec(
        2, 2,
        figure=fig,
        width_ratios=[2.2, 1],
        height_ratios=[1, max(3, n_pairs * 0.55)],
        hspace=0.04,
        wspace=0.06,
        left=0.04, right=0.97,
        top=0.91, bottom=0.05,
    )

    ax_title  = fig.add_subplot(gs[0, :])   # full-width title strip
    ax_table  = fig.add_subplot(gs[1, 0])   # pattern pair table
    ax_null   = fig.add_subplot(gs[1, 1])   # null breakdown

    for ax in [ax_title, ax_table, ax_null]:
        ax.set_facecolor(CARD_BG)
        for spine in ax.spines.values():
            spine.set_visible(False)

    # ── Title strip ───────────────────────────────────────────────────────────
    ax_title.set_xlim(0, 1)
    ax_title.set_ylim(0, 1)
    ax_title.set_xticks([])
    ax_title.set_yticks([])

    ax_title.text(0.018, 0.72, county.upper(),
                  color=TEXT_MAIN, fontsize=22, fontweight="bold",
                  fontfamily="monospace", va="center", transform=ax_title.transAxes)
    ax_title.text(0.018, 0.25, f"{total:,} records  ·  {n_pairs} unique book/page format pairs",
                  color=TEXT_DIM, fontsize=10, va="center", transform=ax_title.transAxes)

    # Accent bar
    ax_title.axhline(0.0, color=ACCENT, linewidth=2.5, xmin=0, xmax=1)

    # ── Pattern pair table ────────────────────────────────────────────────────
    ax_table.set_xlim(0, 1)
    ax_table.set_ylim(0, 1)
    ax_table.set_xticks([])
    ax_table.set_yticks([])

    # Column headers
    col_x    = [0.02, 0.28, 0.54, 0.73, 0.87]
    col_hdrs = ["BOOK PATTERN", "PAGE PATTERN", "COUNT", "  %", "EXAMPLE (book · page)"]
    for x, hdr in zip(col_x, col_hdrs):
        ax_table.text(x, 0.97, hdr, color=ACCENT, fontsize=7.5,
                      fontweight="bold", fontfamily="monospace", va="top",
                      transform=ax_table.transAxes)

    ax_table.plot([0.01, 0.99], [0.945, 0.945], color=GRID_COL, linewidth=1,
                  transform=ax_table.transAxes, clip_on=False)

    row_h  = 0.88 / max(n_pairs, 1)
    y_top  = 0.935

    for i, ((bfp, pfp), count) in enumerate(pairs):
        y      = y_top - i * row_h
        color  = PALETTE[i % len(PALETTE)]
        bex, pex = examples[(bfp, pfp)]
        pct    = count / total * 100

        # Row stripe
        if i % 2 == 0:
            stripe = FancyBboxPatch((0.005, y - row_h * 0.85), 0.989, row_h * 0.9,
                                    boxstyle="round,pad=0.002",
                                    facecolor="#22263A", edgecolor="none",
                                    transform=ax_table.transAxes, zorder=0)
            ax_table.add_patch(stripe)

        # Color swatch
        swatch = FancyBboxPatch((0.005, y - row_h * 0.65), 0.009, row_h * 0.55,
                                boxstyle="round,pad=0.001",
                                facecolor=color, edgecolor="none",
                                transform=ax_table.transAxes, zorder=1)
        ax_table.add_patch(swatch)

        def cell(x, text, bold=False):
            ax_table.text(x, y - row_h * 0.3, text,
                          color=TEXT_MAIN if not bold else color,
                          fontsize=7.8, fontfamily="monospace",
                          fontweight="bold" if bold else "normal",
                          va="center", transform=ax_table.transAxes, clip_on=True)

        cell(0.02,  compress_fp(bfp)[:20], bold=True)
        cell(0.28,  compress_fp(pfp)[:20])
        cell(0.54,  f"{count:,}")
        cell(0.67,  f"{pct:5.1f}%")
        cell(0.78,  f"{bex[:8]} · {pex[:8]}")

    ax_table.text(0.02, 0.01, "BOOK / PAGE FORMAT PAIRS",
                  color=TEXT_DIM, fontsize=6.5, fontfamily="monospace",
                  va="bottom", transform=ax_table.transAxes)

    # ── Null breakdown (donut + legend) ──────────────────────────────────────
    ax_null.set_xlim(0, 1)
    ax_null.set_ylim(0, 1)
    ax_null.set_xticks([])
    ax_null.set_yticks([])

    null_labels  = ["Both present", "Book null", "Page null", "Both null"]
    null_values  = [
        data["null_neither"],
        data["null_book_only"],
        data["null_page_only"],
        data["null_both"],
    ]
    null_colors  = ["#4ECDC4", "#FF6B6B", "#FFE66D", "#8B8FA8"]

    # Filter zero slices
    filtered = [(l, v, c) for l, v, c in zip(null_labels, null_values, null_colors) if v > 0]
    if filtered:
        f_labels, f_values, f_colors = zip(*filtered)
    else:
        f_labels, f_values, f_colors = ["No data"], [1], ["#333"]

    # Donut in an inset axis
    ax_donut = ax_null.inset_axes([0.05, 0.38, 0.90, 0.55])
    ax_donut.set_facecolor(CARD_BG)
    wedges, _ = ax_donut.pie(
        f_values,
        colors=f_colors,
        startangle=90,
        wedgeprops=dict(width=0.45, edgecolor=CARD_BG, linewidth=2),
    )
    ax_donut.set_aspect("equal")

    # Centre label
    ax_donut.text(0, 0, f"{total:,}\nrecords",
                  ha="center", va="center",
                  color=TEXT_MAIN, fontsize=8, fontweight="bold",
                  fontfamily="monospace")

    # Legend below donut
    for i, (label, value, color) in enumerate(zip(f_labels, f_values, f_colors)):
        pct = value / total * 100 if total else 0
        y_leg = 0.33 - i * 0.075
        swatch = FancyBboxPatch((0.08, y_leg - 0.018), 0.06, 0.045,
                                boxstyle="round,pad=0.002",
                                facecolor=color, edgecolor="none",
                                transform=ax_null.transAxes)
        ax_null.add_patch(swatch)
        ax_null.text(0.18, y_leg + 0.005,
                     f"{label}",
                     color=TEXT_MAIN, fontsize=7.5, fontfamily="monospace",
                     va="center", transform=ax_null.transAxes)
        ax_null.text(0.88, y_leg + 0.005,
                     f"{value:,}  ({pct:.1f}%)",
                     color=TEXT_DIM, fontsize=7, fontfamily="monospace",
                     va="center", ha="right", transform=ax_null.transAxes)

    ax_null.text(0.5, 0.975, "NULL BREAKDOWN",
                 color=ACCENT, fontsize=7.5, fontweight="bold",
                 fontfamily="monospace", ha="center", va="top",
                 transform=ax_null.transAxes)

    # ── Save ──────────────────────────────────────────────────────────────────
    out = Path(output_dir) / f"{county}_book_page.png"
    plt.savefig(out, dpi=140, bbox_inches="tight", facecolor=BG)
    plt.close()
    print(f"  [{county}] → {out}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      default="nc_records_assessment.jsonl")
    parser.add_argument("--output-dir", default="outputs")
    args = parser.parse_args()

    Path(args.output_dir).mkdir(exist_ok=True)

    print(f"Streaming {args.input} ...")
    counties = analyze(args.input)
    print(f"Found {len(counties)} counties. Generating plots ...\n")

    for county, data in sorted(counties.items()):
        plot_county(county, data, args.output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
