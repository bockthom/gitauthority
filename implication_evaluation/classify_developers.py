# -*- coding: utf-8 -*-
import argparse
import csv
import os
import sys
from datetime import datetime

import pandas as pd


def parse_commit_date(date_str):
    """Parse git date format: 'Mon Jan 01 12:00:00 2024 +0000'"""
    try:
        return datetime.strptime(date_str.strip(), "%a %b %d %H:%M:%S %Y %z")
    except ValueError:
        return None


def classify_core_developers(input_file, year=None, output_file=None):
    with open(input_file, encoding='utf-8', errors='replace') as fh:
        reader = csv.reader(fh, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
        headers = next(reader)
        rows = list(reader)
    df = pd.DataFrame(rows, columns=headers)

    if year is not None:
        df["parsed_date"] = df["commit_date"].apply(parse_commit_date)
        df = df[df["parsed_date"].apply(lambda d: d is not None and d.year == year)]
        if df.empty:
            print(f"No commits found for year {year}.")
            sys.exit(1)
        print(f"Filtered to {len(df)} commits in {year}.")

    counts = df.groupby("dealiased_author").size().reset_index(name="commit_count")
    counts = counts.sort_values("commit_count", ascending=False).reset_index(drop=True)

    total_commits = counts["commit_count"].sum()
    counts["cumulative_commits"] = counts["commit_count"].cumsum()
    counts["cumulative_pct"] = counts["cumulative_commits"] / total_commits * 100

    # First developer whose inclusion pushes cumulative coverage to >= 80%
    cutoff_idx = int((counts["cumulative_pct"] >= 80.0).idxmax())
    counts["is_core"] = counts.index <= cutoff_idx

    n_core = int(counts["is_core"].sum())
    n_peripheral = len(counts) - n_core
    n_total = len(counts)
    core_commits = int(counts.loc[counts["is_core"], "commit_count"].sum())
    peripheral_commits = int(total_commits) - core_commits

    stats = {
        "total_developers": n_total,
        "core_developers": n_core,
        "peripheral_developers": n_peripheral,
        "total_commits": int(total_commits),
        "core_commits": core_commits,
        "peripheral_commits": peripheral_commits,
        "core_developer_pct": round(n_core / n_total * 100, 2),
        "core_commit_pct": round(core_commits / total_commits * 100, 2),
    }

    print(f"\n20/80 Core Developer Classification")
    print(f"Total developers     : {stats['total_developers']}")
    print(f"  Core developers    : {stats['core_developers']} ({stats['core_developer_pct']:.1f}%)")
    print(f"  Peripheral devs    : {stats['peripheral_developers']} ({100 - stats['core_developer_pct']:.1f}%)")
    print(f"Total commits        : {stats['total_commits']}")
    print(f"  Core commits       : {stats['core_commits']} ({stats['core_commit_pct']:.1f}%)")
    print(f"  Peripheral commits : {stats['peripheral_commits']} ({100 - stats['core_commit_pct']:.1f}%)")

    result = counts[["dealiased_author", "commit_count", "cumulative_pct", "is_core"]].copy()
    result["cumulative_pct"] = result["cumulative_pct"].round(2)

    if output_file:
        result.to_csv(output_file, sep=';', index=False, quoting=csv.QUOTE_NONNUMERIC)
        print(f"Results written to {output_file}")

        stem = output_file.removesuffix(".csv") if output_file.endswith(".csv") else output_file
        stats_file = f"{stem}_stats.csv"
        pd.DataFrame(list(stats.items()), columns=["metric", "value"]).to_csv(
            stats_file, sep=';', index=False, quoting=csv.QUOTE_NONNUMERIC
        )
        print(f"Stats written to {stats_file}")
    else:
        print(result.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description='Classify core developers using the 20/80 commit-count heuristic'
    )
    parser.add_argument('project', type=str, help='Project name (must match name used in extract_commits.py)')
    parser.add_argument('--output-dir', '-d', type=str, default='.',
                        help='Directory to read input from and write output to (default: current directory)')
    parser.add_argument('--year', type=int, default=None,
                        help='Restrict classification to commits from this calendar year')
    args = parser.parse_args()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    input_file = os.path.join(output_dir, f"commit_analysis_{args.project}.csv")
    year_suffix = f"_{args.year}" if args.year else ""
    output_file = os.path.join(output_dir, f"classification_{args.project}{year_suffix}.csv")

    classify_core_developers(input_file, args.year, output_file)


if __name__ == "__main__":
    main()
