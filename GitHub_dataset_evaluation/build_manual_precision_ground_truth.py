#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Turn the stratified 360-username sample into a DRE-format ground truth dataset
for whole-pool evaluation (i.e. via
tool_evaluation/evaluate_gitAuthority_groundtruth.py --dre), instead of the old
per-username evaluation.

Each distinct GitHub username in the sample becomes one dre_id (an anonymized
integer, not the literal username string), and every alias belonging to that
username becomes one woc_id row under that dre_id. Feeding the resulting file to
evaluate_gitAuthority_groundtruth.py runs GitAuthority once over the *entire*
pool of aliases (not scoped to one username at a time), so predicted merges
across different usernames are now real, checkable pairs instead of being
structurally impossible.

Produces three files:
  - <prefix>_raw.csv          : DRE file reflecting the sample as-is (dre_id per
                                 original username, unmodified). Ground truth is
                                 still only as trustworthy as "same username
                                 implies same person" -- i.e. it can still contain
                                 wrong positives (shared/bot/org accounts) and
                                 wrong negatives (one real person using two
                                 accounts).
  - <prefix>_corrected.csv    : Byte-identical starting copy of the same file,
                                 meant to be manually edited: split a wrongly
                                 grouped alias into a new dre_id (or move it to
                                 the dre_id it actually belongs to) to fix the
                                 ground truth by hand.
  - <prefix>_username_map.csv : dre_id -> original GitHub username, kept
                                 separately for your own reference/auditing. NOT
                                 part of the DRE format and not read by the
                                 evaluation script -- the two files above never
                                 carry the literal username.
"""

import argparse
import csv
import os
import random
import sys


def load_sample(input_path):
    """Group (author, organization_account) rows by GitHub username, preserving file order.

    Reads the `git_username` column of the stratified sample CSV -- that's the
    column name the sample file was written with.
    """
    usernames_in_order = []
    seen_usernames = set()
    rows_by_username = {}

    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            username = row['git_username']
            if username not in seen_usernames:
                seen_usernames.add(username)
                usernames_in_order.append(username)
                rows_by_username[username] = []
            rows_by_username[username].append(row['author'])

    return usernames_in_order, rows_by_username


def build_dre_rows(usernames_in_order, rows_by_username, seed):
    """Assign each username an anonymized dre_id and emit one DRE row per alias."""
    rng = random.Random(seed)
    shuffled_usernames = usernames_in_order[:]
    rng.shuffle(shuffled_usernames)

    username_to_dre_id = {username: idx + 1 for idx, username in enumerate(shuffled_usernames)}

    dre_rows = []
    for username in usernames_in_order:
        dre_id = username_to_dre_id[username]
        for author in rows_by_username[username]:
            dre_rows.append({'dre_id': dre_id, 'woc_id': author, 'commit': '', 'time': ''})

    return dre_rows, username_to_dre_id


def write_dre_csv(dre_rows, output_path):
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['dre_id', 'woc_id', 'commit', 'time'])
        writer.writeheader()
        writer.writerows(dre_rows)


def write_username_map(username_to_dre_id, output_path):
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['dre_id', 'username'])
        for username, dre_id in sorted(username_to_dre_id.items(), key=lambda kv: kv[1]):
            writer.writerow([dre_id, username])


def main():
    parser = argparse.ArgumentParser(
        description="Build a DRE-format ground truth pair (raw + a corrected-copy "
                     "template) from the stratified username sample, for whole-pool "
                     "GitAuthority precision/recall evaluation."
    )
    parser.add_argument(
        '--input',
        default='sampled_usernames_stratified_40.csv',
        help="Stratified sample CSV with columns git_username, author, organization_account "
             "(default: sampled_usernames_stratified_40.csv)",
    )
    parser.add_argument(
        '--prefix',
        default='github_final_sample360',
        help="Output filename prefix, should describe the source dataset and sample size "
             "(default: github_final_sample360)",
    )
    parser.add_argument(
        '--output-dir',
        default='.',
        help="Directory to write the output files into, e.g., GitHub_data_final "
             "or GitHub_data_final_privacy-preserving (default: current directory)",
    )
    parser.add_argument(
        '--seed', type=int, default=42,
        help="Random seed used to shuffle dre_id assignment so ids don't leak username order (default: 42)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(args.output_dir):
        print(f"Error: output directory not found: {args.output_dir}", file=sys.stderr)
        sys.exit(1)

    usernames_in_order, rows_by_username = load_sample(args.input)
    dre_rows, username_to_dre_id = build_dre_rows(usernames_in_order, rows_by_username, args.seed)

    raw_path = os.path.join(args.output_dir, f"{args.prefix}_raw.csv")
    corrected_path = os.path.join(args.output_dir, f"{args.prefix}_corrected.csv")
    map_path = os.path.join(args.output_dir, f"{args.prefix}_username_map.csv")

    write_dre_csv(dre_rows, raw_path)
    write_dre_csv(dre_rows, corrected_path)  # identical starting point, to be hand-edited
    write_username_map(username_to_dre_id, map_path)

    print(f"{len(usernames_in_order)} usernames -> {len(dre_rows)} alias rows")
    print(f"Wrote raw ground truth (as-sampled):        {raw_path}")
    print(f"Wrote corrected-copy template (edit this):  {corrected_path}")
    print(f"Wrote dre_id -> username reference map:     {map_path}")
    print()
    print("To fix a wrongly-grouped alias in the corrected file: change its dre_id to")
    print("a dre_id value not used anywhere else in the file (splits it into its own")
    print("singleton identity), or to the dre_id of the group it actually belongs to")
    print("(merges it there instead). Never edit dre_id in the _raw.csv file.")


if __name__ == '__main__':
    main()
