#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to evaluate the gambit algorithm using various datasets.
For each unique identifier (e.g., GitHub_username), runs gambit with all corresponding authors.
Then reports whether all authors are merged and if not, why not.
"""

import sys
import os
import csv
import pandas as pd
import json
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import math
import numpy as np
import signal

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import gambit

# Dataset configurations
DATASET_CONFIGS = {
    'GitHub_data': {
        'csv_path': os.path.join('GitHub_data', 'dataset_after_dedup.csv'),
        'id_column': 'GitHub_username',
        'author_column': 'author',
        'additional_columns': ['organization_account'],
        'filter_column': 'organization_account',
        'filter_value': 0,
        'description': 'GitHub usernames with associated authors (organization_account=0)'
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


def parse_author_string(author_str):
    """Parse author string in format 'name <email>' into name and email."""
    author_str = author_str.strip()

    if '<' in author_str and '>' in author_str:
        parts = author_str.split('<')
        name = parts[0].strip()
        email = parts[1].split('>')[0].strip()
    else:
        # If no email brackets, use the whole string as name
        name = author_str
        email = ""

    return name, email


def create_author_dataframe_gambit(authors_list):
    """Create a DataFrame in the format expected by gambit."""
    data = []
    for idx, author_info in enumerate(authors_list, start=1):
        author_str = author_info['author']
        name, email = parse_author_string(author_str)

        data.append({
            'alias_name': name,
            'alias_email': email
        })

    df = pd.DataFrame(data)
    return df


def run_gambit(authors_list):
    """Run gambit algorithm on a list of authors."""
    # Create DataFrame in expected format
    author_df = create_author_dataframe_gambit(authors_list)

    try:
        # Run the gambit disambiguation algorithm
        merging_df = gambit.disambiguate_aliases(author_df, method='gambit')

        # Transform gambit output (author_id column) to cluster dict
        # Build mapping from index (0-based) to author_id
        uid_to_author_id = {}
        for idx, author_id in enumerate(merging_df['author_id']):
            uid_to_author_id[idx + 1] = author_id  # Convert to 1-based indexing

        # Get unique author_ids (clusters)
        unique_author_ids = set(uid_to_author_id.values())

        # Build clusters: group UIDs by their author_id
        clusters = defaultdict(list)
        for uid, author_id in uid_to_author_id.items():
            clusters[author_id].append(uid)

        return clusters, None
    except Exception as e:
        return None, str(e)


def calculate_per_username_metrics(authors_list, clusters):
    """
    Calculate detailed metrics for a single GitHub username.

    Ground truth: All authors with same GitHub_username should be in one cluster.
    Prediction: Gambit's clustering.

    Returns dict with: precision, recall, f1, accuracy, balanced_accuracy, splitting, lumping, merge_percentage
    """
    if clusters is None or len(authors_list) <= 1:
        # Error case or trivial case (single author)
        return {
            'precision': 1.0 if len(authors_list) == 1 else 0.0,
            'recall': 1.0 if len(authors_list) == 1 else 0.0,
            'f1': 1.0 if len(authors_list) == 1 else 0.0,
            'accuracy': 1.0 if len(authors_list) == 1 else 0.0,
            'balanced_accuracy': 1.0 if len(authors_list) == 1 else 0.0,
            'splitting': 0.0,
            'lumping': 0.0,
            'merge_percentage': 100.0 if len(authors_list) == 1 else 0.0
        }

    num_authors = len(authors_list)

    # Build uid to cluster mapping (convert from clusters dict)
    uid_to_cluster = {}
    for cluster_id, member_uids in clusters.items():
        for uid in member_uids:
            uid_to_cluster[uid - 1] = cluster_id  # Convert to 0-based indexing

    # Calculate pairwise metrics
    # For this GitHub_username, ALL pairs should be merged (ground truth = same username)
    total_pairs = num_authors * (num_authors - 1) // 2
    if total_pairs == 0:
        return calculate_per_username_metrics(authors_list, None)  # Use trivial case

    tp = fp = tn = fn = 0

    for i in range(num_authors):
        for j in range(i + 1, num_authors):
            should_merge = True  # Ground truth: same GitHub_username = should merge
            predicted_merge = (uid_to_cluster.get(i) == uid_to_cluster.get(j))

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


def analyze_merge_results(authors_list, clusters):
    """
    Analyze whether all authors were merged together.
    Args:
        authors_list: List of author info dicts
        clusters: Dict mapping cluster_id to list of UIDs (1-based)
    Returns (all_merged: bool, reason: str)
    """
    if clusters is None:
        return False, "Error running gambit"

    # Get the number of unique authors
    num_authors = len(authors_list)

    if num_authors <= 1:
        return True, ""  # Single author, trivially merged

    # Check if all authors are in a single cluster
    num_clusters = len(clusters)

    if num_clusters == 1:
        return True, ""
    else:
        # Not all merged, build reason string with cluster details
        reason_parts = []
        for cluster_id, member_uids in sorted(clusters.items()):
            try:
                authors_in_cluster = [authors_list[uid-1]['author'] for uid in member_uids]
                reason_parts.append(f"Cluster {cluster_id}: {', '.join(authors_in_cluster)}")
            except (IndexError, TypeError) as e:
                # Debug info
                print(f"DEBUG: Error building reason - cluster_id={cluster_id}, member_uids={member_uids}, num_authors={num_authors}")
                print(f"DEBUG: Error: {e}")
                reason_parts.append(f"Cluster {cluster_id}: {len(member_uids)} members")

        reason = f"{num_clusters} separate clusters found. " + "; ".join(reason_parts)
        return False, reason


def process_chunk(chunk_data):
    """
    Process a chunk of identifiers in a separate process.

    Args:
        chunk_data: Tuple of (chunk_id, identifiers_dict, config)

    Returns:
        List of results for this chunk
    """
    chunk_id, identifiers_dict, config = chunk_data
    id_column = config['id_column']
    results = []

    print(f"[Chunk {chunk_id}] Processing {len(identifiers_dict)} identifiers...")

    for idx, (identifier, authors_list) in enumerate(identifiers_dict.items(), start=1):
        if idx % 100 == 0:
            print(f"[Chunk {chunk_id}] Processed {idx}/{len(identifiers_dict)} identifiers...")

        try:
            # Run gambit
            clusters, error = run_gambit(authors_list)

            if error:
                all_merged = False
                reason = f"Error: {error}"
                metrics = calculate_per_username_metrics(authors_list, None)
            else:
                all_merged, reason = analyze_merge_results(authors_list, clusters)
                metrics = calculate_per_username_metrics(authors_list, clusters)

            results.append({
                id_column: identifier,
                'all_merged': all_merged,
                'reason': reason,
                'num_authors': len(authors_list),
                **metrics  # Include all per-username metrics
            })
        except Exception as e:
            # Catch any exceptions to prevent worker crash
            print(f"[Chunk {chunk_id}] Error processing identifier '{identifier}': {e}")
            results.append({
                id_column: identifier,
                'all_merged': False,
                'reason': f"Processing error: {str(e)[:200]}",
                'num_authors': len(authors_list),
                'precision': 0.0,
                'recall': 0.0,
                'f1': 0.0,
                'accuracy': 0.0,
                'balanced_accuracy': 0.0,
                'splitting': 0.0,
                'lumping': 0.0,
                'merge_percentage': 0.0
            })

    print(f"[Chunk {chunk_id}] Finished processing {len(identifiers_dict)} identifiers")
    return chunk_id, results


def evaluate_identifiers(grouped_data, output_csv, config, limit=None, num_workers=1, filter_stats=None):
    """
    Evaluate gambit on all identifiers.

    Args:
        grouped_data: Dictionary mapping identifier to list of author info
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
        # Serial processing (original behavior)
        results = []
        for idx, (identifier, authors_list) in enumerate(grouped_data.items(), start=1):
            if idx % 100 == 0:
                print(f"Processed {idx}/{total_identifiers} identifiers...")

            # Run gambit
            clusters, error = run_gambit(authors_list)

            if error:
                all_merged = False
                reason = f"Error: {error}"
                metrics = calculate_per_username_metrics(authors_list, None)
            else:
                all_merged, reason = analyze_merge_results(authors_list, clusters)
                metrics = calculate_per_username_metrics(authors_list, clusters)

            results.append({
                id_column: identifier,
                'all_merged': all_merged,
                'reason': reason,
                'num_authors': len(authors_list),
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
                chunks.append((i + 1, chunk_dict, config))

        print(f"Split into {len(chunks)} chunks of ~{chunk_size} identifiers each")

        # Process chunks in parallel
        try:
            # Set up signal handlers to ignore SIGPIPE
            original_sigpipe = signal.signal(signal.SIGPIPE, signal.SIG_IGN) if hasattr(signal, 'SIGPIPE') else None

            # Use maxtasksperchild to prevent memory buildup in workers
            with Pool(processes=num_workers, maxtasksperchild=10) as pool:
                # Use imap_unordered with chunksize=1 for better error handling
                chunk_results = []
                for result in pool.imap_unordered(process_chunk, chunks, chunksize=1):
                    chunk_results.append(result)
                    print(f"Received results from chunk {result[0]}")

            # Restore original signal handler
            if original_sigpipe is not None and hasattr(signal, 'SIGPIPE'):
                signal.signal(signal.SIGPIPE, original_sigpipe)
        except Exception as e:
            print(f"Error in parallel processing: {e}")
            print("Falling back to serial processing for remaining chunks...")
            # Fall back to serial processing if parallel fails
            chunk_results = []
            for chunk in chunks:
                try:
                    result = process_chunk(chunk)
                    chunk_results.append(result)
                except Exception as chunk_error:
                    print(f"Error processing chunk {chunk[0]}: {chunk_error}")
                    continue

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
    id_column = config['id_column']
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
    # Generate summary filename by appending _summary before the .csv extension
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
    # Change to parent directory where mergeAliases expects data files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    os.chdir(parent_dir)
    print(f"Working directory: {os.getcwd()}")

    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python evaluate_gambit.py <dataset_name> [limit] [output_csv] [num_workers] [max_entries]")
        print("  dataset_name: Name of the dataset configuration to use (required)")
        print("  limit: Optional limit on number of identifiers to process (for testing)")
        print("  output_csv: Path to output CSV (optional)")
        print("              Default: evaluation_results_gambit_<dataset_name>[_limit<N>][_max<M>].csv")
        print("  num_workers: Number of parallel workers (default: 1, 0 = number of CPUs)")
        print("  max_entries: Maximum number of entries per identifier (default: no limit)")
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

    # Parse limit, output_csv, num_workers, and max_entries arguments
    limit = None
    output_csv = None
    num_workers = 1
    max_entries = None

    if len(sys.argv) > 2:
        # Try to parse second argument as limit (integer)
        try:
            limit = int(sys.argv[2])
        except ValueError:
            # If not an integer, treat it as output_csv
            output_csv = sys.argv[2]

    if len(sys.argv) > 3:
        # Third argument is output_csv (unless second was output_csv, then this is num_workers)
        if output_csv:
            # Second arg was output_csv, so third is num_workers
            try:
                num_workers = int(sys.argv[3])
            except ValueError:
                print(f"Error: num_workers must be an integer, got: {sys.argv[3]}")
                sys.exit(1)
        else:
            # Second arg was limit, third is output_csv
            output_csv = sys.argv[3]

    if len(sys.argv) > 4:
        # Fourth argument is num_workers
        try:
            num_workers = int(sys.argv[4])
        except ValueError:
            print(f"Error: num_workers must be an integer, got: {sys.argv[4]}")
            sys.exit(1)

    if len(sys.argv) > 5:
        # Fifth argument is max_entries
        try:
            max_entries = int(sys.argv[5])
        except ValueError:
            print(f"Error: max_entries must be an integer, got: {sys.argv[5]}")
            sys.exit(1)

    # Handle num_workers = 0 (use all CPUs)
    if num_workers == 0:
        num_workers = cpu_count()
        print(f"Using all {num_workers} available CPUs")

    # Determine output filename if not provided
    if not output_csv:
        # Default filename includes limit and max_entries if specified and uses gambit prefix
        parts = [f"evaluation_results_gambit_{dataset_name}"]
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

    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found")
        sys.exit(1)

    print(f"Dataset: {dataset_name}")
    print(f"Description: {config['description']}")
    print(f"Input CSV: {input_csv}")
    print(f"Output CSV: {output_csv}")
    if limit:
        print(f"Limit: {limit} identifiers")
    print(f"Workers: {num_workers}")
    if max_entries is not None:
        print(f"Max entries per identifier: {max_entries}")
    else:
        print(f"Max entries per identifier: no limit")
    print()

    # Load data
    df = load_data(input_csv)

    # Group by identifier
    grouped_data, filter_stats = group_by_identifier(df, config, max_entries)

    # Evaluate
    evaluate_identifiers(grouped_data, output_csv, config, limit, num_workers, filter_stats)


if __name__ == "__main__":
    main()
