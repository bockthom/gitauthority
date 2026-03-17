#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to evaluate the ALFAA algorithm on GitHub data using World of Code (WoC).

This script is two-fold:
1. PREPARE PHASE (--prepare-only): Create WoC input from GitHub data
   - Loads GitHub data (GitHub usernames with author aliases)
   - Groups authors by GitHub_username
   - Extracts unique authors and saves to WoC input file

2. EVALUATE PHASE (--woc-output): Process WoC output and evaluate results
   - Loads WoC output (alias assignments)
   - Builds clusters based on WoC assignments
   - Evaluates whether all authors per GitHub_username were merged together

For each unique identifier (GitHub_username), we check if ALFAA merged all corresponding authors.
"""

import sys
import os
import csv
import pandas as pd
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import math
import numpy as np

# Dataset configurations
DATASET_CONFIGS = {
    'GitHub_data': {
        'csv_path': os.path.join('GitHub_data', 'dataset_after_dedup.csv'),
        'id_column': 'GitHub_username',
        'author_column': 'author',
        'additional_columns': ['organization_account'],
        'filter_column': 'organization_account',
        'filter_value': 0,
        'description': 'GitHub usernames with associated author aliases (organization_account=0)'
    },
    # Add more dataset configurations here as needed
    # 'another_dataset': {
    #     'csv_path': 'path/to/another.csv',
    #     'id_column': 'user_id',
    #     'author_column': 'author_name',
    #     'additional_columns': [],
    #     'description': 'Description of this dataset'
    # }
}


def load_data(csv_path):
    """Load the CSV file."""
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"Loaded {len(df)} rows")
    return df


def group_by_identifier(df, config, max_entries=None):
    """Group authors by the identifier column specified in config.

    Returns:
        tuple: (grouped_data, stats_dict) where stats_dict contains:
            - org_removed_count: number of identities removed due to org != filter_value
            - org_removed_avg_authors: average authors per removed org identity
            - org_removed_max_authors: maximum authors per removed org identity
            - max_removed_count: number of identities removed due to max_entries
            - remaining_avg_authors: average authors per remaining identity (before max filtering)
            - remaining_max_authors: maximum authors per remaining identity (before max filtering)
    """
    id_column = config['id_column']
    author_column = config['author_column']
    additional_columns = config.get('additional_columns', [])
    filter_column = config.get('filter_column', None)
    filter_value = config.get('filter_value', None)

    print(f"Grouping authors by '{id_column}'...")

    # Track statistics about filtering
    stats = {
        'initial_count': 0,
        'after_org_filter_count': 0,
        'after_max_filter_count': 0,
        'org_removed_count': 0,
        'org_removed_avg_authors': 0.0,
        'org_removed_median_authors': 0.0,
        'org_removed_std_authors': 0.0,
        'org_removed_max_authors': 0,
        'max_removed_count': 0,
        'remaining_avg_authors': 0.0,
        'remaining_median_authors': 0.0,
        'remaining_std_authors': 0.0,
        'remaining_max_authors': 0
    }

    # First, group ALL data (including org != filter_value) to get statistics
    all_grouped = defaultdict(list)
    for _, row in df.iterrows():
        identifier = row[id_column]
        author = row[author_column]

        extra_data = {}
        for col in additional_columns:
            if col in row:
                extra_data[col] = row[col]

        all_grouped[identifier].append({
            'author': author,
            **extra_data
        })

    # Record initial count before any filtering
    stats['initial_count'] = len(all_grouped)

    # Calculate statistics for org != filter_value identities
    if filter_column and filter_value is not None:
        org_removed_identities = []
        for identifier, authors_list in all_grouped.items():
            # Check if any author in this identifier has org != filter_value
            if authors_list and authors_list[0].get(filter_column) != filter_value:
                org_removed_identities.append(identifier)

        stats['org_removed_count'] = len(org_removed_identities)
        if org_removed_identities:
            author_counts = [len(all_grouped[ident]) for ident in org_removed_identities]
            stats['org_removed_avg_authors'] = np.mean(author_counts)
            stats['org_removed_median_authors'] = np.median(author_counts)
            stats['org_removed_std_authors'] = np.std(author_counts)
            stats['org_removed_max_authors'] = max(author_counts)

    # Apply filter if specified
    grouped = defaultdict(list)
    if filter_column and filter_value is not None:
        original_count = len(df)
        df = df[df[filter_column] == filter_value]
        print(f"Filtered by {filter_column}={filter_value}: {original_count} -> {len(df)} rows")

    for _, row in df.iterrows():
        identifier = row[id_column]
        author = row[author_column]

        # Collect additional columns if specified
        extra_data = {}
        for col in additional_columns:
            if col in row:
                extra_data[col] = row[col]

        grouped[identifier].append({
            'author': author,
            **extra_data
        })

    # Calculate average and max for remaining identities (before max filtering)
    if grouped:
        author_counts = [len(authors_list) for authors_list in grouped.values()]
        stats['remaining_avg_authors'] = np.mean(author_counts)
        stats['remaining_median_authors'] = np.median(author_counts)
        stats['remaining_std_authors'] = np.std(author_counts)
        stats['remaining_max_authors'] = max(author_counts)

    # Record count after org filtering, before max threshold filtering
    stats['after_org_filter_count'] = len(grouped)

    # Apply max_entries filter and track removals
    if max_entries is not None:
        identifiers_to_remove = []
        for identifier, authors_list in grouped.items():
            if len(authors_list) > max_entries:
                print(f"Warning: More than {max_entries} entries for identifier '{identifier}'")
                identifiers_to_remove.append(identifier)
                print(f"Removed identifier '{identifier}' from data due to excessive entries")

        for identifier in identifiers_to_remove:
            grouped.pop(identifier, None)

        stats['max_removed_count'] = len(identifiers_to_remove)

    # Record final count after all filtering
    stats['after_max_filter_count'] = len(grouped)

    print(f"Found {len(grouped)} unique identifiers")
    return grouped, stats


def prepare_woc_input(grouped_data, output_file):
    """
    Extract unique authors from grouped data and prepare WoC input file.

    Args:
        grouped_data: Dictionary mapping identifier to list of author info
        output_file: File to write the list of unique authors for WoC lookup

    Returns:
        Dictionary mapping author string to list of identifiers using that author
    """
    # Build mapping of unique authors to their identifiers
    author_to_identifiers = defaultdict(list)

    for identifier, authors_list in grouped_data.items():
        for author_info in authors_list:
            author_str = author_info['author']
            author_to_identifiers[author_str].append(identifier)

    # Write unique authors to file for WoC lookup
    unique_authors = sorted(author_to_identifiers.keys())
    # prepend "GitHub_dataset_evaluation" directory to output_file if not absolute path
    if not os.path.isabs(output_file):
        output_file = os.path.join('GitHub_dataset_evaluation', output_file)
    with open(output_file, 'w', encoding='utf-8') as f:
        for author in unique_authors:
            f.write(f"{author}\n")

    print(f"\nPrepared {len(unique_authors)} unique authors for World of Code lookup")
    print(f"Authors written to: {output_file}")
    print(f"Total author-identifier relationships: {sum(len(ids) for ids in author_to_identifiers.values())}")

    return author_to_identifiers


def parse_woc_output(woc_output_file):
    """
    Parse World of Code output and build alias mapping.

    Args:
        woc_output_file: File containing WoC output in format:
                        "searched name <searched email>;assigned name <assigned email>"

    Returns:
        Dictionary mapping searched alias string to assigned alias string
    """
    alias_mapping = {}

    print(f"Loading World of Code output from {woc_output_file}...")

    try:
        with open(woc_output_file, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: WoC output file not found: {woc_output_file}")
        sys.exit(1)

    for line_num, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue

        if ';' not in line:
            print(f"Warning: Line {line_num} missing semicolon separator: {line[:100]}")
            continue

        parts = line.split(';')
        if len(parts) != 2:
            print(f"Warning: Line {line_num} has {len(parts)} parts (expected 2): {line[:100]}")
            continue

        searched_alias = parts[0].strip()
        assigned_alias = parts[1].strip()

        alias_mapping[searched_alias] = assigned_alias

    print(f"Parsed {len(alias_mapping)} WoC assignments")
    return alias_mapping


def build_clusters_from_woc(grouped_data, woc_mapping):
    """
    Build clusters for each identifier based on World of Code assignments.

    Args:
        grouped_data: Dictionary mapping identifier to list of author info
        woc_mapping: Dictionary mapping searched author -> assigned author

    Returns:
        Dictionary mapping identifier to list of (author, cluster_id) tuples
    """
    identifier_clusters = {}

    for identifier, authors_list in grouped_data.items():
        author_clusters = []

        for author_info in authors_list:
            author_str = author_info['author']

            # Get assigned cluster from WoC
            if author_str in woc_mapping:
                cluster_id = woc_mapping[author_str]
            else:
                # If no mapping found, author clusters to itself
                cluster_id = author_str

            author_clusters.append((author_str, cluster_id))

        identifier_clusters[identifier] = author_clusters

    return identifier_clusters


def calculate_per_username_metrics(author_clusters):
    """
    Calculate detailed metrics for a single GitHub username.

    Ground truth: All authors with same GitHub_username should be in one cluster.
    Prediction: ALFAA's clustering.

    Returns dict with: precision, recall, f1, accuracy, balanced_accuracy, splitting, lumping, merge_percentage
    """
    num_authors = len(author_clusters)

    if num_authors <= 1:
        # Trivial case (single author)
        return {
            'precision': 1.0,
            'recall': 1.0,
            'f1': 1.0,
            'accuracy': 1.0,
            'balanced_accuracy': 1.0,
            'splitting': 0.0,
            'lumping': 0.0,
            'merge_percentage': 100.0
        }

    # Build uid to cluster mapping
    uid_to_cluster = {}
    for idx, (author, cluster_id) in enumerate(author_clusters):
        uid_to_cluster[idx] = cluster_id

    # Calculate pairwise metrics
    # For this GitHub_username, ALL pairs should be merged (ground truth = same username)
    total_pairs = num_authors * (num_authors - 1) // 2
    if total_pairs == 0:
        return calculate_per_username_metrics([])  # Use trivial case

    tp = fp = tn = fn = 0

    for i in range(num_authors):
        for j in range(i + 1, num_authors):
            should_merge = True  # Ground truth: same GitHub_username = should merge
            predicted_merge = (uid_to_cluster[i] == uid_to_cluster[j])

            if should_merge and predicted_merge:
                tp += 1
            elif should_merge and not predicted_merge:
                fn += 1
            elif not should_merge and predicted_merge:
                fp += 1
            else:
                tn += 1

    # Calculate metrics
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (tp + tn) / (tp + fp + tn + fn) if (tp + fp + tn + fn) > 0 else 0.0

    specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0
    balanced_accuracy = (recall + specificity) / 2.0

    splitting = fn / (tp + fn) if (tp + fn) > 0 else 0.0
    lumping = fp / (tp + fn) if (tp + fn) > 0 else 0.0

    merge_percentage = 100.0 * tp / total_pairs if total_pairs > 0 else 0.0

    return {
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'accuracy': accuracy,
        'balanced_accuracy': balanced_accuracy,
        'splitting': splitting,
        'lumping': lumping,
        'merge_percentage': merge_percentage
    }


def analyze_merge_results(identifier, author_clusters):
    """
    Analyze whether all authors for an identifier were merged together.

    Args:
        identifier: The identifier (e.g., GitHub_username)
        author_clusters: List of (author, cluster_id) tuples

    Returns:
        Tuple of (all_merged: bool, reason: str, num_authors: int, num_clusters: int)
    """
    num_authors = len(author_clusters)

    if num_authors <= 1:
        return True, "", num_authors, 1  # Single author, trivially merged

    # Get unique clusters
    unique_clusters = set(cluster_id for _, cluster_id in author_clusters)
    num_clusters = len(unique_clusters)

    if num_clusters == 1:
        return True, "", num_authors, num_clusters
    else:
        # Not all merged, build reason string with cluster details
        clusters_dict = defaultdict(list)
        for author, cluster_id in author_clusters:
            clusters_dict[cluster_id].append(author)

        reason_parts = []
        for cluster_id, members in sorted(clusters_dict.items()):
            reason_parts.append(f"Cluster '{cluster_id[:50]}...': {', '.join(members)}")

        reason = f"{num_clusters} separate clusters found. " + "; ".join(reason_parts)
        return False, reason, num_authors, num_clusters


def process_chunk(chunk_data):
    """
    Process a chunk of identifiers in a separate process.

    Args:
        chunk_data: Tuple of (chunk_id, identifiers_dict, woc_mapping, config)

    Returns:
        Tuple of (chunk_id, results_list)
    """
    chunk_id, identifiers_dict, woc_mapping, config = chunk_data
    id_column = config['id_column']
    results = []

    print(f"[Chunk {chunk_id}] Processing {len(identifiers_dict)} identifiers...")

    # Build clusters for this chunk
    identifier_clusters = build_clusters_from_woc(identifiers_dict, woc_mapping)

    for idx, (identifier, author_clusters) in enumerate(identifier_clusters.items(), start=1):
        if idx % 100 == 0:
            print(f"[Chunk {chunk_id}] Processed {idx}/{len(identifiers_dict)} identifiers...")

        # Analyze merge results
        all_merged, reason, num_authors, num_clusters = analyze_merge_results(identifier, author_clusters)
        metrics = calculate_per_username_metrics(author_clusters)

        results.append({
            id_column: identifier,
            'all_merged': all_merged,
            'reason': reason,
            'num_authors': num_authors,
            'num_clusters': num_clusters,
            **metrics  # Include all per-username metrics
        })

    print(f"[Chunk {chunk_id}] Finished processing {len(identifiers_dict)} identifiers")
    return chunk_id, results


def evaluate_identifiers(grouped_data, woc_mapping, output_csv, config, limit=None, num_workers=1, filter_stats=None):
    """
    Evaluate ALFAA (via WoC) on all identifiers.

    Args:
        grouped_data: Dictionary mapping identifier to list of author info
        woc_mapping: Dictionary mapping author -> assigned cluster
        output_csv: Path to output CSV file
        config: Dataset configuration
        limit: Optional limit on number of identifiers to process (for testing)
        num_workers: Number of parallel workers to use (default: 1)
        filter_stats: Dictionary containing filtering statistics from group_by_identifier
    """
    id_column = config['id_column']

    # Apply limit if specified
    if limit:
        grouped_data_items = list(grouped_data.items())[:limit]
        grouped_data = dict(grouped_data_items)

    total_identifiers = len(grouped_data)

    print(f"\nProcessing {total_identifiers} identifiers using {num_workers} worker(s)...")

    if num_workers == 1:
        # Serial processing
        identifier_clusters = build_clusters_from_woc(grouped_data, woc_mapping)

        results = []
        for idx, (identifier, author_clusters) in enumerate(identifier_clusters.items(), start=1):
            if idx % 100 == 0:
                print(f"Processed {idx}/{total_identifiers} identifiers...")

            # Analyze merge results
            all_merged, reason, num_authors, num_clusters = analyze_merge_results(identifier, author_clusters)
            metrics = calculate_per_username_metrics(author_clusters)

            results.append({
                id_column: identifier,
                'all_merged': all_merged,
                'reason': reason,
                'num_authors': num_authors,
                'num_clusters': num_clusters,
                **metrics  # Include all per-username metrics
            })
    else:
        # Parallel processing
        # Split grouped_data into chunks
        items = list(grouped_data.items())
        chunk_size = math.ceil(len(items) / num_workers)
        chunks = []

        for i in range(num_workers):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(items))
            if start_idx < len(items):
                chunk_dict = dict(items[start_idx:end_idx])
                chunks.append((i + 1, chunk_dict, woc_mapping, config))

        print(f"Split into {len(chunks)} chunks of ~{chunk_size} identifiers each")

        # Process chunks in parallel
        with Pool(processes=num_workers) as pool:
            chunk_results = pool.map(process_chunk, chunks)

        # Write individual chunk results
        base_output = output_csv.replace('.csv', '')
        for chunk_id, chunk_data in chunk_results:
            chunk_file = f"{base_output}_chunk{chunk_id}.csv"
            print(f"Writing chunk {chunk_id} results to {chunk_file}...")
            with open(chunk_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[id_column, 'all_merged', 'reason'])
                writer.writeheader()
                for result in chunk_data:
                    writer.writerow({
                        id_column: result[id_column],
                        'all_merged': result['all_merged'],
                        'reason': result['reason']
                    })

        # Merge all results
        results = []
        for chunk_id, chunk_data in sorted(chunk_results):
            results.extend(chunk_data)

    print(f"\nFinished processing {len(results)} identifiers")

    # Write results to CSV
    print(f"Writing results to {output_csv}...")
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[id_column, 'all_merged', 'reason'])
        writer.writeheader()
        for result in results:
            writer.writerow({
                id_column: result[id_column],
                'all_merged': result['all_merged'],
                'reason': result['reason']
            })

    print(f"Results written to {output_csv}")

    # Write per-username metrics to separate CSV
    per_username_csv = output_csv.replace('.csv', '_per_username.csv')
    print(f"Writing per-username metrics to {per_username_csv}...")
    with open(per_username_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [id_column, 'num_authors', 'merge_percentage', 'precision', 'recall',
                     'f1', 'accuracy', 'balanced_accuracy', 'splitting', 'lumping']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow({
                id_column: result[id_column],
                'num_authors': result['num_authors'],
                'merge_percentage': f"{result['merge_percentage']:.2f}",
                'precision': f"{result['precision']:.4f}",
                'recall': f"{result['recall']:.4f}",
                'f1': f"{result['f1']:.4f}",
                'accuracy': f"{result['accuracy']:.4f}",
                'balanced_accuracy': f"{result['balanced_accuracy']:.4f}",
                'splitting': f"{result['splitting']:.4f}",
                'lumping': f"{result['lumping']:.4f}"
            })

    print(f"Per-username metrics written to {per_username_csv}")

    # Calculate summary statistics
    total = len(results)
    merged = sum(1 for r in results if r['all_merged'])
    not_merged = total - merged

    # Additional statistics
    identifiers_with_multiple_authors = sum(1 for r in results if r['num_authors'] > 1)
    merged_with_multiple = sum(1 for r in results if r['all_merged'] and r['num_authors'] > 1)

    # Calculate aggregate metrics across all usernames
    avg_merge_percentage = sum(r['merge_percentage'] for r in results) / total if total > 0 else 0.0
    avg_precision = sum(r['precision'] for r in results) / total if total > 0 else 0.0
    avg_recall = sum(r['recall'] for r in results) / total if total > 0 else 0.0
    avg_f1 = sum(r['f1'] for r in results) / total if total > 0 else 0.0
    avg_accuracy = sum(r['accuracy'] for r in results) / total if total > 0 else 0.0
    avg_balanced_accuracy = sum(r['balanced_accuracy'] for r in results) / total if total > 0 else 0.0
    avg_splitting = sum(r['splitting'] for r in results) / total if total > 0 else 0.0
    avg_lumping = sum(r['lumping'] for r in results) / total if total > 0 else 0.0

    # Print summary statistics
    print(f"\n=== SUMMARY ===")
    print(f"Total {id_column}s: {total}")
    print(f"All authors merged: {merged} ({100*merged/total:.1f}%)")
    print(f"Not all merged: {not_merged} ({100*not_merged/total:.1f}%)")
    print(f"\n{id_column}s with multiple authors: {identifiers_with_multiple_authors}")
    if identifiers_with_multiple_authors > 0:
        print(f"Successfully merged (multi-author only): {merged_with_multiple} ({100*merged_with_multiple/identifiers_with_multiple_authors:.1f}%)")

    print(f"\n=== AGGREGATE METRICS (AVERAGE ACROSS ALL USERNAMES) ===")
    print(f"Merge Percentage:  {avg_merge_percentage:.2f}%")
    print(f"Precision:         {avg_precision:.4f}")
    print(f"Recall:            {avg_recall:.4f}")
    print(f"F1 Score:          {avg_f1:.4f}")
    print(f"Accuracy:          {avg_accuracy:.4f}")
    print(f"Balanced Accuracy: {avg_balanced_accuracy:.4f}")
    print(f"Splitting:         {avg_splitting:.4f}")
    print(f"Lumping:           {avg_lumping:.4f}")

    # Print filtering statistics if available
    if filter_stats:
        print(f"\n=== FILTERING STATISTICS ===")
        print(f"Initial {id_column}s (before filtering): {filter_stats['initial_count']}")
        print(f"After org != 0 filter: {filter_stats['after_org_filter_count']}")
        print(f"After max threshold filter: {filter_stats['after_max_filter_count']}")
        print(f"\nIdentities removed (org != 0): {filter_stats['org_removed_count']}")
        if filter_stats['org_removed_count'] > 0:
            print(f"Average authors per removed org identity: {filter_stats['org_removed_avg_authors']:.2f}")
            print(f"Median authors per removed org identity: {filter_stats['org_removed_median_authors']:.2f}")
            print(f"Std dev authors per removed org identity: {filter_stats['org_removed_std_authors']:.2f}")
            print(f"Maximum authors per removed org identity: {filter_stats['org_removed_max_authors']}")
        print(f"Identities removed (max threshold): {filter_stats['max_removed_count']}")
        print(f"Average authors per remaining identity (before max filtering): {filter_stats['remaining_avg_authors']:.2f}")
        print(f"Median authors per remaining identity (before max filtering): {filter_stats['remaining_median_authors']:.2f}")
        print(f"Std dev authors per remaining identity (before max filtering): {filter_stats['remaining_std_authors']:.2f}")
        print(f"Maximum authors per remaining identity (before max filtering): {filter_stats['remaining_max_authors']}")

    # Write summary statistics to a separate file
    if output_csv.endswith('.csv'):
        summary_csv = output_csv[:-4] + '_summary.csv'
    else:
        summary_csv = output_csv + '_summary.csv'

    print(f"\nWriting summary statistics to {summary_csv}...")
    with open(summary_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Count', 'Percentage'])
        writer.writerow([f'Total {id_column}s', total, '100.0'])
        writer.writerow(['All authors merged', merged, f'{100*merged/total:.1f}'])
        writer.writerow(['Not all merged', not_merged, f'{100*not_merged/total:.1f}'])
        writer.writerow([f'{id_column}s with multiple authors', identifiers_with_multiple_authors,
                        f'{100*identifiers_with_multiple_authors/total:.1f}' if total > 0 else '0.0'])
        if identifiers_with_multiple_authors > 0:
            writer.writerow(['Successfully merged (multi-author only)', merged_with_multiple,
                           f'{100*merged_with_multiple/identifiers_with_multiple_authors:.1f}'])

        # Add aggregate metrics
        writer.writerow([])  # Empty row for separation
        writer.writerow(['=== AGGREGATE METRICS (AVERAGE ACROSS ALL USERNAMES) ===', '', ''])
        writer.writerow(['Merge Percentage', '', f'{avg_merge_percentage:.2f}%'])
        writer.writerow(['Precision', '', f'{avg_precision:.4f}'])
        writer.writerow(['Recall', '', f'{avg_recall:.4f}'])
        writer.writerow(['F1 Score', '', f'{avg_f1:.4f}'])
        writer.writerow(['Accuracy', '', f'{avg_accuracy:.4f}'])
        writer.writerow(['Balanced Accuracy', '', f'{avg_balanced_accuracy:.4f}'])
        writer.writerow(['Splitting', '', f'{avg_splitting:.4f}'])
        writer.writerow(['Lumping', '', f'{avg_lumping:.4f}'])

        # Add filtering statistics if available
        if filter_stats:
            writer.writerow([])  # Empty row for separation
            writer.writerow(['=== FILTERING STATISTICS ===', '', ''])
            writer.writerow([f'Initial {id_column}s (before filtering)', filter_stats['initial_count'], ''])
            writer.writerow([f'After org != 0 filter', filter_stats['after_org_filter_count'], ''])
            writer.writerow([f'After max threshold filter', filter_stats['after_max_filter_count'], ''])
            writer.writerow([])  # Empty row for separation
            writer.writerow(['Identities removed (org != 0)', filter_stats['org_removed_count'], ''])
            if filter_stats['org_removed_count'] > 0:
                writer.writerow(['Avg authors per removed org identity', f"{filter_stats['org_removed_avg_authors']:.2f}", ''])
                writer.writerow(['Median authors per removed org identity', f"{filter_stats['org_removed_median_authors']:.2f}", ''])
                writer.writerow(['Std dev authors per removed org identity', f"{filter_stats['org_removed_std_authors']:.2f}", ''])
                writer.writerow(['Max authors per removed org identity', filter_stats['org_removed_max_authors'], ''])
            writer.writerow(['Identities removed (max threshold)', filter_stats['max_removed_count'], ''])
            writer.writerow(['Avg authors per remaining identity (before max)', f"{filter_stats['remaining_avg_authors']:.2f}", ''])
            writer.writerow(['Median authors per remaining identity (before max)', f"{filter_stats['remaining_median_authors']:.2f}", ''])
            writer.writerow(['Std dev authors per remaining identity (before max)', f"{filter_stats['remaining_std_authors']:.2f}", ''])
            writer.writerow(['Max authors per remaining identity (before max)', filter_stats['remaining_max_authors'], ''])
    print(f"Summary statistics written to {summary_csv}")


def list_datasets():
    """List all available dataset configurations."""
    print("\nAvailable datasets:")
    for name, config in DATASET_CONFIGS.items():
        print(f"  - {name}: {config['description']}")
        print(f"    CSV path: {config['csv_path']}")
        print(f"    ID column: {config['id_column']}")
        print(f"    Author column: {config['author_column']}")
        print()


def main():
    # Change to parent directory where data files are located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    os.chdir(parent_dir)
    print(f"Working directory: {os.getcwd()}")

    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python evaluate_alfaa_github.py <dataset_name> [OPTIONS]")
        print("\nTwo-fold operation:")
        print("  PHASE 1 - Prepare WoC input:")
        print("    python evaluate_alfaa_github.py <dataset_name> --prepare-only [--woc-input <file>] [--max-entries <N>]")
        print("\n  PHASE 2 - Evaluate with WoC output:")
        print("    python evaluate_alfaa_github.py <dataset_name> --woc-output <file> [OPTIONS]")
        print("\nArguments:")
        print("  dataset_name: Name of the dataset configuration to use (required)")
        print("\nOptions:")
        print("  --prepare-only: Only prepare WoC input file, do not evaluate")
        print("  --woc-input <file>: Path to WoC input file (default: GitHub_dataset_evaluation/woc_input_alfaa_<dataset>.txt)")
        print("  --woc-output <file>: Path to WoC output file (required for evaluation)")
        print("  --output <file>: Path to output CSV (default: evaluation_results_alfaa_<dataset>[_limit<N>][_max<M>].csv)")
        print("  --limit <N>: Limit number of identifiers to process (for testing)")
        print("  --num-workers <N>: Number of parallel workers (default: 1, 0 = number of CPUs)")
        print("  --max-entries <N>: Maximum number of authors per identifier (default: no limit)")
        print()
        list_datasets()
        sys.exit(1)

    dataset_name = sys.argv[1]

    # Check if dataset configuration exists
    if dataset_name not in DATASET_CONFIGS:
        print(f"Error: Unknown dataset '{dataset_name}'")
        list_datasets()
        sys.exit(1)

    config = DATASET_CONFIGS[dataset_name]

    # Get script directory to resolve relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_csv = os.path.join(script_dir, config['csv_path'])

    # Parse command line options
    prepare_only = False
    woc_input_file = None
    woc_output_file = None
    output_csv = None
    limit = None
    num_workers = 1
    max_entries = None

    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg == '--prepare-only':
            prepare_only = True
            i += 1
        elif arg == '--woc-input':
            if i + 1 >= len(sys.argv):
                print("Error: --woc-input requires a file path")
                sys.exit(1)
            woc_input_file = sys.argv[i + 1]
            i += 2
        elif arg == '--woc-output':
            if i + 1 >= len(sys.argv):
                print("Error: --woc-output requires a file path")
                sys.exit(1)
            woc_output_file = sys.argv[i + 1]
            i += 2
        elif arg == '--output':
            if i + 1 >= len(sys.argv):
                print("Error: --output requires a file path")
                sys.exit(1)
            output_csv = sys.argv[i + 1]
            i += 2
        elif arg == '--limit':
            if i + 1 >= len(sys.argv):
                print("Error: --limit requires a number")
                sys.exit(1)
            try:
                limit = int(sys.argv[i + 1])
            except ValueError:
                print(f"Error: --limit must be an integer, got: {sys.argv[i + 1]}")
                sys.exit(1)
            i += 2
        elif arg == '--num-workers':
            if i + 1 >= len(sys.argv):
                print("Error: --num-workers requires a number")
                sys.exit(1)
            try:
                num_workers = int(sys.argv[i + 1])
            except ValueError:
                print(f"Error: --num-workers must be an integer, got: {sys.argv[i + 1]}")
                sys.exit(1)
            i += 2
        elif arg == '--max-entries':
            if i + 1 >= len(sys.argv):
                print("Error: --max-entries requires a number")
                sys.exit(1)
            try:
                max_entries = int(sys.argv[i + 1])
            except ValueError:
                print(f"Error: --max-entries must be an integer, got: {sys.argv[i + 1]}")
                sys.exit(1)
            i += 2
        else:
            print(f"Error: Unknown option '{arg}'")
            sys.exit(1)

    # Handle num_workers = 0 (use all CPUs)
    if num_workers == 0:
        num_workers = cpu_count()
        print(f"Using all {num_workers} available CPUs")

    # Set default WoC input filename if not provided
    if not woc_input_file:
        parts = [f"woc_input_alfaa_{dataset_name}"]
        if limit:
            parts.append(f"_limit{limit}")
        if max_entries is not None:
            parts.append(f"_max{max_entries}")
        parts.append(".txt")
        woc_input_file = "".join(parts)

    # Set default output filename if not provided
    if not output_csv:
        parts = [f"evaluation_results_alfaa_{dataset_name}"]
        if limit:
            parts.append(f"_limit{limit}")
        if max_entries is not None:
            parts.append(f"_max{max_entries}")
        parts.append(".csv")
        output_csv = os.path.join('GitHub_dataset_evaluation', "".join(parts))
    else:
        # If custom output provided, ensure it's in GitHub_dataset_evaluation directory if relative path
        if not os.path.isabs(output_csv) and not output_csv.startswith(os.path.join('GitHub_dataset_evaluation', '')):
            output_csv = os.path.join('GitHub_dataset_evaluation', output_csv)

    # Ensure woc_output_file has GitHub_dataset_evaluation prefix if it's a relative path
    if woc_output_file and not os.path.isabs(woc_output_file) and not woc_output_file.startswith(os.path.join('GitHub_dataset_evaluation', '')):
        woc_output_file = os.path.join('GitHub_dataset_evaluation', woc_output_file)

    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found")
        sys.exit(1)

    print(f"Dataset: {dataset_name}")
    print(f"Description: {config['description']}")
    print(f"Input CSV: {input_csv}")
    if limit:
        print(f"Limit: {limit} identifiers")
    if max_entries is not None:
        print(f"Max entries per identifier: {max_entries}")
    else:
        print(f"Max entries per identifier: no limit")
    print()

    # Load data
    df = load_data(input_csv)

    # Group by identifier
    grouped_data, filter_stats = group_by_identifier(df, config, max_entries)

    # Apply limit if specified (for both phases)
    if limit:
        grouped_data_items = list(grouped_data.items())[:limit]
        grouped_data = dict(grouped_data_items)
        print(f"Limited to {len(grouped_data)} identifiers for processing")

    # PHASE 1: Prepare WoC input
    print("\n" + "="*70)
    print("PHASE 1: Preparing World of Code input")
    print("="*70)

    author_to_identifiers = prepare_woc_input(grouped_data, woc_input_file)

    if prepare_only:
        print("\n" + "="*70)
        print("NEXT STEPS:")
        print("="*70)
        print(f"1. Use the authors in '{woc_input_file}' to query World of Code")
        print(f"   Example: cat {woc_input_file} | ~/lookup/getValues -vV2409 a2A > woc_output_alfaa.txt")
        print("2. WoC will return output in format:")
        print("   'searched name <email>;assigned name <assigned email>'")
        print("3. Run this script again with --woc-output flag:")
        print(f"   python {os.path.basename(__file__)} {dataset_name} \\")
        print(f"     --woc-output woc_output_alfaa.txt \\")
        print(f"     --output {output_csv}")
        if num_workers > 1:
            print(f"     --num-workers {num_workers}")
        print("="*70)
        return

    # PHASE 2: Evaluate with WoC output
    if not woc_output_file:
        print("\nError: --woc-output is required for evaluation")
        print("Use --prepare-only to just generate the input file for World of Code")
        sys.exit(1)

    print("\n" + "="*70)
    print("PHASE 2: Evaluating with World of Code output")
    print("="*70)

    # Parse WoC output
    woc_mapping = parse_woc_output(woc_output_file)

    # Check coverage
    total_authors = len(author_to_identifiers)
    mapped_authors = sum(1 for author in author_to_identifiers.keys() if author in woc_mapping)
    print(f"WoC coverage: {mapped_authors}/{total_authors} authors ({100*mapped_authors/total_authors:.1f}%)")

    # Evaluate
    print(f"Output CSV: {output_csv}")
    print(f"Workers: {num_workers}")
    evaluate_identifiers(grouped_data, woc_mapping, output_csv, config, limit=None,
                        num_workers=num_workers, filter_stats=filter_stats)


if __name__ == "__main__":
    main()
