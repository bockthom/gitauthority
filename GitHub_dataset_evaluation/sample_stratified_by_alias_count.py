#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stratified sampling of GitHub usernames by number of aliases.

Reads the GitHub_data_final evaluation dataset (git_username, author, organization_account),
where each row is one alias of a username, and draws a uniform random sample of N
usernames for each alias-count stratum in [min_aliases, max_aliases].

Since usernames are grouped contiguously in the input file (all rows of a username appear
together, though usernames are not globally sorted), this is done in a single streaming
pass using reservoir sampling per stratum, so the full ~1.3GB file never has to be
loaded into memory.

Use --extend-from to grow an already-labeled sample: usernames listed in the given
summary CSV are kept as-is, and only the additional slots needed to reach
--n-per-stratum are reservoir-sampled from the remaining, not-yet-sampled pool.
"""

import argparse
import csv
import os
import random
import sys
from collections import defaultdict


def load_existing(summary_path):
    """
    Load a previously written summary CSV (num_aliases, git_username) and return
    a dict alias_count -> set of usernames that must be kept in the new sample.
    """
    existing = defaultdict(set)
    with open(summary_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing[int(row["num_aliases"])].add(row["git_username"])
    return existing


def stratified_sample(input_path, min_aliases, max_aliases, n_per_stratum, seed, existing=None):
    """
    Stream the input CSV and reservoir-sample usernames for each alias count in
    [min_aliases, max_aliases], topping each stratum up to n_per_stratum.

    Usernames listed in `existing` (dict alias_count -> set of usernames) are always
    kept; only the remaining slots are reservoir-sampled from the rest of the pool.

    Returns:
        header: CSV header row of the input file
        reservoirs: dict alias_count -> list of (username, [rows]) tuples (kept + newly sampled)
        seen_counts: dict alias_count -> total number of usernames seen with that count
        total_usernames: total number of distinct usernames seen in the file
        missing: dict alias_count -> set of usernames from `existing` never found in the file
    """
    existing = existing or {}
    rng = random.Random(seed)

    kept_rows = {k: {} for k in range(min_aliases, max_aliases + 1)}
    new_reservoirs = {k: [] for k in range(min_aliases, max_aliases + 1)}
    new_targets = {
        k: max(0, n_per_stratum - len(existing.get(k, ()))) for k in range(min_aliases, max_aliases + 1)
    }
    seen_counts = defaultdict(int)
    seen_new_counts = defaultdict(int)
    total_usernames = 0

    with open(input_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)

        current_username = None
        current_rows = []

        def flush_group():
            nonlocal total_usernames
            if current_username is None:
                return
            total_usernames += 1
            k = len(current_rows)
            if min_aliases <= k <= max_aliases:
                seen_counts[k] += 1
                if current_username in existing.get(k, ()):
                    kept_rows[k][current_username] = list(current_rows)
                else:
                    target = new_targets[k]
                    if target > 0:
                        seen_new_counts[k] += 1
                        reservoir = new_reservoirs[k]
                        if len(reservoir) < target:
                            reservoir.append((current_username, list(current_rows)))
                        else:
                            j = rng.randrange(seen_new_counts[k])
                            if j < target:
                                reservoir[j] = (current_username, list(current_rows))

        for row in reader:
            username = row[0]
            if username != current_username:
                flush_group()
                current_username = username
                current_rows = []
            current_rows.append(row)

        flush_group()  # last group

    reservoirs = {}
    missing = {}
    for k in range(min_aliases, max_aliases + 1):
        kept = [(u, kept_rows[k][u]) for u in existing.get(k, ()) if u in kept_rows[k]]
        missing[k] = set(existing.get(k, ())) - set(kept_rows[k])
        reservoirs[k] = kept + new_reservoirs[k]

    return header, reservoirs, seen_counts, total_usernames, missing


def write_output(header, reservoirs, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header + ["num_aliases"])
        for k in sorted(reservoirs):
            for username, rows in reservoirs[k]:
                for row in rows:
                    writer.writerow(row + [k])


def write_summary(reservoirs, summary_path):
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["num_aliases", "git_username"])
        for k in sorted(reservoirs):
            for username, _rows in reservoirs[k]:
                writer.writerow([k, username])


def build_stats_lines(total_usernames, reservoirs, seen_counts, missing):
    lines = [f"Scanned {total_usernames:,} distinct usernames.", ""]
    lines.append("Sample sizes per stratum (sampled / available):")
    for k in sorted(reservoirs):
        lines.append(f"  {k:>2} aliases: {len(reservoirs[k]):>3} / {seen_counts.get(k, 0)}")
    if any(missing.values()):
        lines.append("")
        lines.append("WARNING: usernames from --extend-from not found in this input file:")
        for k in sorted(missing):
            if missing[k]:
                lines.append(f"  {k:>2} aliases: {sorted(missing[k])}")
    return lines


def main():
    parser = argparse.ArgumentParser(
        description="Stratified sample of GitHub usernames by number of aliases."
    )
    parser.add_argument(
        "--input",
        default="GitHub_data_final/evaluation_dataset_only_users_after_filtering_org_after_dedup.csv",
        help="Path to the GitHub_data_final CSV (git_username, author, organization_account)",
    )
    parser.add_argument(
        "--min-aliases", type=int, default=2, help="Minimum number of aliases per username (default: 2)"
    )
    parser.add_argument(
        "--max-aliases", type=int, default=10, help="Maximum number of aliases per username (default: 10)"
    )
    parser.add_argument(
        "--n-per-stratum", type=int, default=20, help="Target number of usernames per alias count (default: 20)"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    parser.add_argument(
        "--extend-from",
        default=None,
        help="Path to a previous *_summary.csv whose usernames must be kept; only the "
        "extra slots needed to reach --n-per-stratum are newly sampled",
    )
    parser.add_argument(
        "--output",
        default="sampled_usernames_stratified.csv",
        help="Output CSV with the sampled usernames' full alias rows",
    )
    parser.add_argument(
        "--summary-output",
        default="sampled_usernames_stratified_summary.csv",
        help="Output CSV listing just num_aliases, git_username for the sample",
    )
    parser.add_argument(
        "--stats-output",
        default="sampled_usernames_stratified_stats.txt",
        help="Output text file with the sampling statistics (default: sampled_usernames_stratified_stats.txt)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if args.min_aliases > args.max_aliases:
        print("Error: --min-aliases must be <= --max-aliases", file=sys.stderr)
        sys.exit(1)

    existing = None
    if args.extend_from:
        if not os.path.isfile(args.extend_from):
            print(f"Error: --extend-from file not found: {args.extend_from}", file=sys.stderr)
            sys.exit(1)
        existing = load_existing(args.extend_from)
        n_existing = sum(len(v) for v in existing.values())
        print(f"Loaded {n_existing} already-sampled usernames to keep from {args.extend_from}")

    print(f"Streaming {args.input} ...")
    header, reservoirs, seen_counts, total_usernames, missing = stratified_sample(
        args.input, args.min_aliases, args.max_aliases, args.n_per_stratum, args.seed, existing
    )

    write_output(header, reservoirs, args.output)
    write_summary(reservoirs, args.summary_output)

    stats_lines = build_stats_lines(total_usernames, reservoirs, seen_counts, missing)
    print("\n" + "\n".join(stats_lines))

    with open(args.stats_output, "w", encoding="utf-8") as f:
        f.write("\n".join(stats_lines) + "\n")

    print(f"\nWrote sampled alias rows to: {args.output}")
    print(f"Wrote sample summary to:     {args.summary_output}")
    print(f"Wrote sampling statistics to: {args.stats_output}")


if __name__ == "__main__":
    main()
