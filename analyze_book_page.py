"""
Analyze and visualize book vs page patterns from NC records JSONL file.
- Case-aware fingerprinting: U=uppercase letter, l=lowercase letter, N=digit
- Scatter plot per county: book number vs page number, colored by format pair
- Pattern pair table showing how book and page formats relate

Usage:
    python analyze_book_page.py --input nc_records_assessment.jsonl
"""

import json
import re
import argparse
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


# ── Fingerprinting ────────────────────────────────────────────────────────────

def fingerprint(s):
    """
    Case-aware structural fingerprint.
      Uppercase letter → U
      Lowercase letter → l
      Digit            → N
      Everything else kept as-is (hyphens, slashes, spaces, dots)

    Examples:
        "1398"   → "NNNN"
        "Ab-12"  → "Ul-NN"
        "OR"     → "UU"
        "or"     → "ll"
        "3A"     → "NU"
    """
    s = str(s).strip()
    result = []
    for ch in s:
        if ch.isupper():
            result.append('U')
        elif ch.islower():
            result.append('l')
        elif ch.isdigit():
            result.append('N')
        else:
            result.append(ch)
    return ''.join(result)


def derive_regex(fp):
    """Convert fingerprint to a usable regex, compressing runs."""
    pattern = re.sub(
        r'(U+|l+|N+|[^UlN]+)',
        lambda m: (
            f'[A-Z]{{{len(m.group())}}}' if set(m.group()) == {'U'} else
            f'[a-z]{{{len(m.group())}}}' if set(m.group()) == {'l'} else
            f'\\d{{{len(m.group())}}}' if set(m.group()) == {'N'} else
            re.escape(m.group())
        ),
        fp
    )
    return f'^{pattern}$'


def compress_fp_display(fp):
    """Human-readable compressed fingerprint: NNNN → N(4), UU → U(2)"""
    return re.sub(r'(.)\1+', lambda m: f'{m.group(1)}({len(m.group())})', fp)


# ── Streaming ─────────────────────────────────────────────────────────────────

def stream_records(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


# ── Core analysis ─────────────────────────────────────────────────────────────

def analyze(filepath):
    """
    Stream once. Per county, track:
      - pair_counter: how often each (book_fp, page_fp) combination appears
      - pair_examples: one real (book, page) value per pair
      - scatter_points: numeric (book, page) pairs for plotting
    """
    counties = defaultdict(lambda: {
        "record_count": 0,
        "pair_counter": Counter(),
        "pair_examples": {},
        "scatter_points": [],
        "null_both": 0,
        "null_book_only": 0,
        "null_page_only": 0,
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

        book_fp = fingerprint(book_str) if book_str else "NULL"
        page_fp = fingerprint(page_str) if page_str else "NULL"
        pair = (book_fp, page_fp)

        c["pair_counter"][pair] += 1
        c["pair_examples"].setdefault(pair, (book_str, page_str))

        # Scatter: only when both are purely numeric
        if book_str and page_str and book_str.isdigit() and page_str.isdigit():
            label = f"{compress_fp_display(book_fp)} | {compress_fp_display(page_fp)}"
            c["scatter_points"].append((int(book_str), int(page_str), label))

    return counties


# ── Visualization ─────────────────────────────────────────────────────────────

COLORS = [
    "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
    "#42d4f4", "#f032e6", "#bfef45", "#469990", "#dcbeff",
    "#9A6324", "#800000", "#aaffc3", "#000075", "#a9a9a9",
]


def plot_county(county, data, output_dir):
    points = data["scatter_points"]
    if not points:
        print(f"  [{county}] No numeric book+page pairs — skipping scatter.")
        return

    label_to_points = defaultdict(list)
    for bk, pg, label in points:
        label_to_points[label].append((bk, pg))

    fig, ax = plt.subplots(figsize=(10, 7))

    for i, (label, pts) in enumerate(sorted(label_to_points.items(), key=lambda x: -len(x[1]))):
        color = COLORS[i % len(COLORS)]
        bks = [p[0] for p in pts]
        pgs = [p[1] for p in pts]
        ax.scatter(bks, pgs, c=color, label=f"{label}  (n={len(pts)})",
                   alpha=0.5, s=15, linewidths=0)

    ax.set_title(
        f"{county.upper()} — Book vs Page\n(colored by format pair: book_fp | page_fp)",
        fontsize=13, fontweight="bold"
    )
    ax.set_xlabel("Book (numeric value)", fontsize=11)
    ax.set_ylabel("Page (numeric value)", fontsize=11)
    ax.legend(title="book pattern | page pattern", fontsize=8,
              title_fontsize=9, loc="upper left", framealpha=0.9)

    plt.tight_layout()
    out = Path(output_dir) / f"scatter_{county}.png"
    plt.savefig(out, dpi=130, bbox_inches="tight")
    plt.close()
    print(f"  [{county}] Scatter saved → {out}")


# ── Console summary ───────────────────────────────────────────────────────────

def print_summary(counties):
    for county, data in sorted(counties.items()):
        print(f"\n{'='*70}")
        print(f"  {county.upper()}  ({data['record_count']} records)")
        print(f"{'='*70}")
        print(f"  Nulls — both: {data['null_both']}  "
              f"book only: {data['null_book_only']}  "
              f"page only: {data['null_page_only']}")
        print(f"\n  {'BOOK PATTERN':<22} {'PAGE PATTERN':<22} {'N':>6}  {'%':>5}  EXAMPLE")
        print(f"  {'-'*22} {'-'*22} {'-'*6}  {'-'*5}  {'-'*20}")

        total = data["record_count"]
        for (bfp, pfp), count in data["pair_counter"].most_common():
            bex, pex = data["pair_examples"][(bfp, pfp)]
            pct = round(count / total * 100, 1)
            print(f"  {compress_fp_display(bfp):<22} {compress_fp_display(pfp):<22} "
                  f"{count:>6}  {pct:>4}%  ({bex}, {pex})")


# ── JSON output ───────────────────────────────────────────────────────────────

def build_json_output(counties):
    output = {}
    for county, data in sorted(counties.items()):
        pairs = []
        for (bfp, pfp), count in data["pair_counter"].most_common():
            bex, pex = data["pair_examples"][(bfp, pfp)]
            pairs.append({
                "book_pattern": compress_fp_display(bfp),
                "book_regex": derive_regex(bfp),
                "page_pattern": compress_fp_display(pfp),
                "page_regex": derive_regex(pfp),
                "book_example": bex,
                "page_example": pex,
                "count": count,
                "percentage": round(count / data["record_count"] * 100, 2),
            })
        output[county] = {
            "record_count": data["record_count"],
            "null_both": data["null_both"],
            "null_book_only": data["null_book_only"],
            "null_page_only": data["null_page_only"],
            "pairs": pairs,
        }
    return output


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="nc_records_assessment.jsonl")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--json-output", default="outputs/book_page_patterns.json")
    args = parser.parse_args()

    Path(args.output_dir).mkdir(exist_ok=True)

    print(f"Streaming {args.input} ...")
    counties = analyze(args.input)
    print(f"Found {len(counties)} counties.\n")

    print_summary(counties)

    print("\nGenerating scatter plots ...")
    for county, data in sorted(counties.items()):
        plot_county(county, data, args.output_dir)

    output = build_json_output(counties)
    with open(args.json_output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\nJSON saved → {args.json_output}")


if __name__ == "__main__":
    main()
