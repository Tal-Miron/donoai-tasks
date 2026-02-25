"""
Visualize doc_type vs doc_category relationship from the NC records JSONL file.
Produces a heatmap showing co-occurrence counts.

Usage:
    python visualize_doc_types.py --input nc_records_assessment.jsonl --top 20
"""

import json
import argparse
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import pandas as pd


def stream_records(filepath):
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def build_cooccurrence(filepath, top_n=20):
    """
    Stream the JSONL and build a co-occurrence counter:
      {(doc_category, doc_type): count}
    Also tracks the most common doc_types to limit columns.
    """
    cooccurrence = Counter()
    doc_type_counts = Counter()

    for record in stream_records(filepath):
        doc_type = record.get("doc_type")
        doc_category = record.get("doc_category")

        if not doc_type or not doc_category:
            continue

        doc_type = str(doc_type).strip().upper()
        doc_category = str(doc_category).strip().upper()

        cooccurrence[(doc_category, doc_type)] += 1
        doc_type_counts[doc_type] += 1

    # Limit to top N most common doc_types (keeps heatmap readable)
    top_doc_types = [dt for dt, _ in doc_type_counts.most_common(top_n)]

    return cooccurrence, top_doc_types


def build_dataframe(cooccurrence, top_doc_types):
    """Convert co-occurrence counter into a DataFrame suitable for heatmap."""
    # Get all categories that appear with any of the top doc_types
    categories = sorted(set(
        cat for (cat, dt) in cooccurrence.keys()
        if dt in top_doc_types
    ))

    data = []
    for cat in categories:
        row = {}
        for dt in top_doc_types:
            row[dt] = cooccurrence.get((cat, dt), 0)
        data.append(row)

    df = pd.DataFrame(data, index=categories, columns=top_doc_types)

    # Drop rows that are all zeros (categories with no top doc_types)
    df = df.loc[(df != 0).any(axis=1)]

    return df


def plot_heatmap(df, output_path="doc_type_category_heatmap.png"):
    fig_width = max(16, len(df.columns) * 0.7)
    fig_height = max(8, len(df.index) * 0.5)

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    sns.heatmap(
        df,
        ax=ax,
        cmap="YlOrRd",
        linewidths=0.3,
        linecolor="lightgrey",
        annot=True,
        fmt="d",
        annot_kws={"size": 7},
        cbar_kws={"label": "Record Count"},
    )

    ax.set_title(
        "Doc Type vs Doc Category Co-occurrence\n(Top doc_types by frequency)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.set_xlabel("doc_type", fontsize=11, labelpad=10)
    ax.set_ylabel("doc_category", fontsize=11, labelpad=10)

    # Rotate x labels for readability
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved heatmap to: {output_path}")
    plt.show()


def print_summary(cooccurrence, top_doc_types):
    """Print a text summary of the relationship."""
    print("\n--- Doc Type → Category Mapping Summary ---")

    # For each top doc_type, which categories does it map to?
    dt_to_cats = defaultdict(Counter)
    for (cat, dt), count in cooccurrence.items():
        if dt in top_doc_types:
            dt_to_cats[dt][cat] += count

    for dt in top_doc_types:
        cats = dt_to_cats[dt]
        if len(cats) == 1:
            cat, count = list(cats.items())[0]
            print(f"  {dt:40s} → {cat} ({count} records) [clean 1:1]")
        else:
            print(f"  {dt:40s} → MULTIPLE CATEGORIES:")
            for cat, count in cats.most_common():
                print(f"      {cat} ({count} records)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="nc_records_assessment.jsonl")
    parser.add_argument("--top", type=int, default=20, help="Top N doc_types to show")
    parser.add_argument("--output", default="doc_type_category_heatmap.png")
    args = parser.parse_args()

    print(f"Streaming {args.input}...")
    cooccurrence, top_doc_types = build_cooccurrence(args.input, top_n=args.top)
    print(f"Found {len(set(dt for _, dt in cooccurrence))} unique doc_types")
    print(f"Found {len(set(cat for cat, _ in cooccurrence))} unique doc_categories")

    print_summary(cooccurrence, top_doc_types)

    df = build_dataframe(cooccurrence, top_doc_types)
    plot_heatmap(df, output_path=args.output)

if __name__ == "__main__":
    main()