#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to evaluate ALFAA algorithm against manual ground truth data.

ALFAA requires World of Code (WoC) to run, so this script:
1. Loads ground truth data
2. Extracts unique aliases that need to be looked up in WoC
3. Expects WoC output to be provided (format: "searched name <email>;assigned name <email>")
4. Evaluates ALFAA's clustering against ground truth
"""

import sys
import os
import csv
import pandas as pd
import numpy as np
from collections import defaultdict
import argparse


class GroundTruthLoader:
    """Loads ground truth data in different formats."""

    @staticmethod
    def load_matrix_format(developers_file, groundtruth_file):
        """
        Load ground truth in matrix format (Gambit style).

        Args:
            developers_file: CSV file with alias list (columns: rec_name, rec_email)
            groundtruth_file: CSV file with NxN matrix where non-zero indicates merge

        Returns:
            Tuple of (aliases_df, pairs_list) where:
            - aliases_df: DataFrame with columns [alias_full, alias_name, alias_email, alias_id]
            - pairs_list: List of (id1, id2, should_merge) tuples
        """
        # Load aliases
        aliases_df = pd.read_csv(developers_file, encoding='utf-8')
        aliases_df.columns = ['alias_name', 'alias_email']
        aliases_df['alias_id'] = range(len(aliases_df))

        # Create alias_full column (Name <email> format)
        aliases_df['alias_full'] = aliases_df.apply(
            lambda row: f"{row['alias_name']} <{row['alias_email']}>", axis=1
        )

        # Reorder columns
        aliases_df = aliases_df[['alias_id', 'alias_full', 'alias_name', 'alias_email']]

        # Load ground truth matrix
        gt_matrix = pd.read_csv(groundtruth_file, header=None, encoding='utf-8').values

        # Extract pairs from matrix
        pairs = []
        n = gt_matrix.shape[0]
        for i in range(n):
            for j in range(i + 1, n):  # Upper triangle only to avoid duplicates
                should_merge = int(gt_matrix[i, j] > 0)
                pairs.append((i, j, should_merge))

        return aliases_df, pairs

    @staticmethod
    def load_pairwise_format(pairwise_file):
        """
        Load ground truth in pairwise format (ALFAA style).

        Args:
            pairwise_file: CSV file where each row is: alias1;alias2;similarity;label
                          (label in last column: 1 = merge, 0 = don't merge)

        Returns:
            Tuple of (aliases_df, pairs_list) where:
            - aliases_df: DataFrame with columns [alias_full, alias_name, alias_email, alias_id]
            - pairs_list: List of (id1, id2, should_merge) tuples
        """
        # Read pairwise file
        df = pd.read_csv(pairwise_file, sep=';', header=None, encoding='utf-8')

        # Extract alias columns (first two) and label (fourth column)
        # Column format: alias1;alias2;similarity_score;label
        label_col = 3  # Fourth column (0-indexed)

        # Extract unique aliases
        alias_to_id = {}
        next_id = 0
        pairs = []

        for _, row in df.iterrows():
            alias1_str = str(row[0]).strip()
            alias2_str = str(row[1]).strip()
            should_merge = int(row[label_col])

            # Get or create ID for alias1
            if alias1_str not in alias_to_id:
                alias_to_id[alias1_str] = next_id
                next_id += 1

            # Get or create ID for alias2
            if alias2_str not in alias_to_id:
                alias_to_id[alias2_str] = next_id
                next_id += 1

            id1 = alias_to_id[alias1_str]
            id2 = alias_to_id[alias2_str]

            pairs.append((id1, id2, should_merge))

        # Create aliases DataFrame
        aliases_data = []
        for alias_str, alias_id in sorted(alias_to_id.items(), key=lambda x: x[1]):
            # Parse "Name <email>" format
            if '<' in alias_str and '>' in alias_str:
                name = alias_str.split('<')[0].strip()
                email = alias_str.split('<')[1].split('>')[0].strip()
            else:
                name = alias_str
                email = ''

            aliases_data.append({
                'alias_id': alias_id,
                'alias_full': alias_str,
                'alias_name': name,
                'alias_email': email
            })

        aliases_df = pd.DataFrame(aliases_data)

        return aliases_df, pairs

    @staticmethod
    def load_dre_format(dre_file):
        """
        Load ground truth in DRE format (DREUser.csv).

        Args:
            dre_file: CSV file with columns: dre_id, woc_id, commit, time
                     All woc_ids and dre_ids with same dre_id are considered one cluster

        Returns:
            Tuple of (aliases_df, pairs_list) where:
            - aliases_df: DataFrame with columns [alias_id, alias_full, alias_name, alias_email, dre_id]
            - pairs_list: List of (id1, id2, should_merge) tuples
        """
        # Read DRE file
        df = pd.read_csv(dre_file, encoding='utf-8')

        # Collect all unique identities
        aliases_data = []
        next_id = 0

        # Build mapping of woc_id to dre_id
        woc_to_dre = {}
        for _, row in df.iterrows():
            woc_id = row['woc_id']
            dre_id = row['dre_id']
            if woc_id not in woc_to_dre:
                woc_to_dre[woc_id] = str(dre_id).strip()

        # Create aliases only from unique woc_ids
        unique_woc_ids = df['woc_id'].unique()
        for woc_id in unique_woc_ids:
            woc_id_str = str(woc_id).strip()

            # Parse "Name <email>" format
            if '<' in woc_id_str and '>' in woc_id_str:
                name = woc_id_str.split('<')[0].strip()
                email = woc_id_str.split('<')[1].split('>')[0].strip()
            else:
                name = woc_id_str
                email = ''

            aliases_data.append({
                'alias_id': next_id,
                'alias_full': woc_id_str,
                'alias_name': name,
                'alias_email': email,
                'dre_id': woc_to_dre[woc_id]
            })
            next_id += 1

        aliases_df = pd.DataFrame(aliases_data)

        # Generate all pairwise comparisons
        pairs = []
        n = len(aliases_df)
        for i in range(n):
            for j in range(i + 1, n):
                # Same dre_id means they should merge
                dre_id_i = aliases_df.iloc[i]['dre_id']
                dre_id_j = aliases_df.iloc[j]['dre_id']
                should_merge = 1 if dre_id_i == dre_id_j else 0
                pairs.append((i, j, should_merge))

        return aliases_df, pairs

    @staticmethod
    def load_wiese_format(wiese_file, asf_developers_file=None):
        """
        Load ground truth in Wiese format (apache.community).

        Args:
            wiese_file: Text file where each line has format:
                       id#=#name1#;#name2#;#...##=#email1#;#email2#;#...
                       All names/emails in a line belong to the same person
            asf_developers_file: Optional file with actual input data (Name <email> format).
                                If provided, loads aliases from this file and enriches them
                                with cluster_id from wiese_file.

        Returns:
            Tuple of (aliases_df, pairs_list) where:
            - aliases_df: DataFrame with columns [alias_id, alias_full, alias_name, alias_email, cluster_id]
            - pairs_list: List of (id1, id2, should_merge) tuples
        """
        import re

        # If asf_developers_file is provided, use a different approach:
        # Load aliases from AsfDevelopers.txt and enrich with ground truth cluster_id
        if asf_developers_file:
            print(f"Loading input data from {asf_developers_file}...")
            aliases_df = GroundTruthLoader.load_asf_developers(asf_developers_file)

            # Build email -> cluster_id mapping from wiese file
            print(f"Loading ground truth clusters from {wiese_file}...")
            email_to_cluster = {}
            clusters_found = 0

            with open(wiese_file, 'r', encoding='latin-1') as f:
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        parts = line.split('##=#')
                        if len(parts) != 2:
                            continue

                        # Parse cluster ID
                        if parts[0].endswith('#='):
                            cluster_id = parts[0][:-2].strip()
                        else:
                            id_and_names = parts[0].split('#=#')
                            if len(id_and_names) != 2:
                                continue
                            cluster_id = id_and_names[0].strip()

                        # Parse emails
                        emails_str = parts[1]
                        emails = [e.strip() for e in emails_str.split('#;#') if e.strip()]

                        # Map all emails to this cluster
                        for email in emails:
                            email_to_cluster[email] = cluster_id

                    except Exception:
                        continue

            print(f"Found {len(email_to_cluster)} emails in ground truth")

            # Enrich aliases with cluster_id
            aliases_df['cluster_id'] = aliases_df['alias_email'].map(email_to_cluster)

            # Filter out aliases without ground truth
            before_count = len(aliases_df)
            aliases_df = aliases_df[aliases_df['cluster_id'].notna()].copy()
            aliases_df = aliases_df.reset_index(drop=True)
            aliases_df['alias_id'] = range(len(aliases_df))  # Reassign IDs
            after_count = len(aliases_df)

            print(f"Matched {after_count} aliases from input data to ground truth")
            print(f"Skipped {before_count - after_count} aliases without ground truth")
            print(f"Ground truth clusters: {aliases_df['cluster_id'].nunique()}")

            # Generate pairwise comparisons
            pairs = []
            n = len(aliases_df)
            for i in range(n):
                for j in range(i + 1, n):
                    cluster_i = aliases_df.iloc[i]['cluster_id']
                    cluster_j = aliases_df.iloc[j]['cluster_id']
                    should_merge = 1 if cluster_i == cluster_j else 0
                    pairs.append((i, j, should_merge))

            return aliases_df, pairs

        # Original approach: extract emails from wiese file only (no input data file)
        aliases_data = []
        next_id = 0

        with open(wiese_file, 'r', encoding='latin-1') as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue

                # Parse format: id#=#names##=#emails
                try:
                    # Split by ##=# to separate names and emails sections
                    parts = line.split('##=#')
                    if len(parts) != 2:
                        print(f"Warning: Skipping malformed line {line_num + 1}: {line[:100]}")
                        continue

                    # Parse ID and names from first part
                    # Handle empty names section (format: id#=##=#emails)
                    if parts[0].endswith('#='):
                        cluster_id = parts[0][:-2].strip()  # Remove trailing '#='
                    else:
                        id_and_names = parts[0].split('#=#')
                        if len(id_and_names) != 2:
                            print(f"Warning: Skipping malformed line {line_num + 1}: {line[:100]}")
                            continue
                        cluster_id = id_and_names[0].strip()

                    # Parse emails from second part
                    emails_str = parts[1]
                    emails = [e.strip() for e in emails_str.split('#;#') if e.strip()]

                    # Create aliases from emails only (no Cartesian product with names)
                    # Each email becomes one alias with empty name
                    for email in emails:
                        aliases_data.append({
                            'alias_id': next_id,
                            'alias_full': f"<{email}>",  # Format: <email>
                            'alias_name': '',  # Empty name - only use emails
                            'alias_email': email,
                            'cluster_id': cluster_id
                        })
                        next_id += 1

                except Exception as e:
                    print(f"Warning: Error parsing line {line_num + 1}: {e}")
                    continue

        aliases_df = pd.DataFrame(aliases_data)

        # Generate all pairwise comparisons
        pairs = []
        n = len(aliases_df)
        for i in range(n):
            for j in range(i + 1, n):
                # Same cluster_id means they should merge
                cluster_i = aliases_df.iloc[i]['cluster_id']
                cluster_j = aliases_df.iloc[j]['cluster_id']
                should_merge = 1 if cluster_i == cluster_j else 0
                pairs.append((i, j, should_merge))

        return aliases_df, pairs

    @staticmethod
    def load_asf_developers(asf_developers_file):
        """
        Load input data from AsfDevelopers.txt file.

        Args:
            asf_developers_file: Text file where each line has format:
                                "Name <email>"

        Returns:
            DataFrame with columns [alias_id, alias_full, alias_name, alias_email]
        """
        import re
        aliases_data = []
        next_id = 0

        with open(asf_developers_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip().strip('"')
                if not line:
                    continue

                # Parse "Name <email>" format
                match = re.search(r'(.+?)\s*<([^>]+)>', line)
                if match:
                    name = match.group(1).strip()
                    email = match.group(2).strip()

                    aliases_data.append({
                        'alias_id': next_id,
                        'alias_full': f"{name} <{email}>",
                        'alias_name': name,
                        'alias_email': email
                    })
                    next_id += 1

        return pd.DataFrame(aliases_data)


def prepare_woc_input(aliases_df, output_file):
    """
    Prepare list of aliases for World of Code lookup.

    Args:
        aliases_df: DataFrame with columns [alias_full, alias_name, alias_email, alias_id]
        output_file: File to write the list of aliases for WoC lookup

    Returns:
        List of alias strings in "Name <email>" format
    """
    alias_list = aliases_df['alias_full'].tolist()

    # Write to file for convenience
    with open(output_file, 'w', encoding='utf-8') as f:
        for alias in alias_list:
            f.write(f"{alias}\n")

    print(f"\nPrepared {len(alias_list)} aliases for World of Code lookup")
    print(f"Aliases written to: {output_file}")
    print("\nEach line contains one alias in format: Name <email>")

    return alias_list


def parse_woc_output(woc_output_data):
    """
    Parse World of Code output and build alias clustering.

    Args:
        woc_output_data: List of strings in format:
                        "searched name <searched email>;assigned name <assigned email>"

    Returns:
        Dictionary mapping searched alias string to assigned alias string
    """
    alias_mapping = {}

    for line in woc_output_data:
        line = line.strip()
        if not line or ';' not in line:
            continue

        parts = line.split(';')
        if len(parts) != 2:
            print(f"Warning: Skipping malformed line: {line}")
            continue

        searched_alias = parts[0].strip()
        assigned_alias = parts[1].strip()

        alias_mapping[searched_alias] = assigned_alias

    return alias_mapping


def build_clusters_from_woc(aliases_df, woc_mapping):
    """
    Build clusters based on World of Code assignments.

    Args:
        aliases_df: DataFrame with columns [alias_full, alias_name, alias_email, alias_id]
        woc_mapping: Dictionary mapping searched alias -> assigned alias

    Returns:
        Dictionary mapping alias_id to cluster_id (assigned alias string)
    """
    alias_to_cluster = {}

    for idx, row in aliases_df.iterrows():
        alias_id = row['alias_id']
        alias_full = row['alias_full']

        # Get assigned alias from WoC
        if alias_full in woc_mapping:
            assigned_alias = woc_mapping[alias_full]
            alias_to_cluster[alias_id] = assigned_alias
        else:
            # If no mapping found, alias clusters to itself
            print(f"Warning: No WoC mapping found for alias ID {alias_id}: {alias_full}")
            alias_to_cluster[alias_id] = alias_full

    return alias_to_cluster


def evaluate_predictions(pairs, alias_to_cluster, aliases_df):
    """
    Evaluate predictions against ground truth.

    Args:
        pairs: List of (id1, id2, should_merge) tuples
        alias_to_cluster: Dictionary mapping alias_id to cluster_id
        aliases_df: DataFrame with alias information

    Returns:
        Dictionary with evaluation metrics (precision, recall, f1, TP, FP, TN, FN) and error lists
    """
    true_positives = 0
    false_positives = 0
    true_negatives = 0
    false_negatives = 0

    false_positive_pairs = []
    false_negative_pairs = []

    for id1, id2, should_merge in pairs:
        # Check if both aliases have cluster assignments
        if id1 not in alias_to_cluster or id2 not in alias_to_cluster:
            continue

        predicted_merge = (alias_to_cluster[id1] == alias_to_cluster[id2])

        if should_merge == 1:  # Ground truth: should merge
            if predicted_merge:
                true_positives += 1
            else:
                false_negatives += 1
                # Store false negative details
                alias1 = aliases_df[aliases_df['alias_id'] == id1].iloc[0]
                alias2 = aliases_df[aliases_df['alias_id'] == id2].iloc[0]
                false_negative_pairs.append({
                    'id1': id1,
                    'id2': id2,
                    'alias1_full': alias1['alias_full'],
                    'alias1_name': alias1['alias_name'],
                    'alias1_email': alias1['alias_email'],
                    'alias2_full': alias2['alias_full'],
                    'alias2_name': alias2['alias_name'],
                    'alias2_email': alias2['alias_email']
                })
        else:  # Ground truth: should not merge
            if predicted_merge:
                false_positives += 1
                # Store false positive details
                alias1 = aliases_df[aliases_df['alias_id'] == id1].iloc[0]
                alias2 = aliases_df[aliases_df['alias_id'] == id2].iloc[0]
                false_positive_pairs.append({
                    'id1': id1,
                    'id2': id2,
                    'alias1_full': alias1['alias_full'],
                    'alias1_name': alias1['alias_name'],
                    'alias1_email': alias1['alias_email'],
                    'alias2_full': alias2['alias_full'],
                    'alias2_name': alias2['alias_name'],
                    'alias2_email': alias2['alias_email']
                })
            else:
                true_negatives += 1

    # Calculate metrics
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0.0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (true_positives + true_negatives) / (true_positives + false_positives + true_negatives + false_negatives) if (true_positives + false_positives + true_negatives + false_negatives) > 0 else 0.0

    # Calculate specificity and balanced accuracy
    specificity = true_negatives / (true_negatives + false_positives) if (true_negatives + false_positives) > 0 else 0.0
    balanced_accuracy = (recall + specificity) / 2.0

    # Calculate splitting and lumping metrics
    splitting = false_negatives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0.0
    lumping = false_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0.0

    return {
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'accuracy': accuracy,
        'balanced_accuracy': balanced_accuracy,
        'splitting': splitting,
        'lumping': lumping,
        'true_positives': true_positives,
        'false_positives': false_positives,
        'true_negatives': true_negatives,
        'false_negatives': false_negatives,
        'total_pairs': len(pairs),
        'false_positive_pairs': false_positive_pairs,
        'false_negative_pairs': false_negative_pairs
    }


def print_results(metrics):
    """Print evaluation results."""
    print("\n=== EVALUATION RESULTS ===")
    print(f"Precision:         {metrics['precision']:.4f}")
    print(f"Recall:            {metrics['recall']:.4f}")
    print(f"F1 Score:          {metrics['f1']:.4f}")
    print(f"Accuracy:          {metrics['accuracy']:.4f}")
    print(f"Balanced Accuracy: {metrics['balanced_accuracy']:.4f}")
    print(f"Splitting:         {metrics['splitting']:.4f}")
    print(f"Lumping:           {metrics['lumping']:.4f}")
    print(f"\nConfusion Matrix:")
    print(f"  True Positives:  {metrics['true_positives']}")
    print(f"  False Positives: {metrics['false_positives']}")
    print(f"  True Negatives:  {metrics['true_negatives']}")
    print(f"  False Negatives: {metrics['false_negatives']}")
    print(f"  Total Pairs:     {metrics['total_pairs']}")


def save_results(metrics, output_file):
    """Save evaluation results to CSV."""
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value'])
        writer.writerow(['Precision', f"{metrics['precision']:.4f}"])
        writer.writerow(['Recall', f"{metrics['recall']:.4f}"])
        writer.writerow(['F1 Score', f"{metrics['f1']:.4f}"])
        writer.writerow(['Accuracy', f"{metrics['accuracy']:.4f}"])
        writer.writerow(['Balanced Accuracy', f"{metrics['balanced_accuracy']:.4f}"])
        writer.writerow(['Splitting', f"{metrics['splitting']:.4f}"])
        writer.writerow(['Lumping', f"{metrics['lumping']:.4f}"])
        writer.writerow([])
        writer.writerow(['Confusion Matrix', ''])
        writer.writerow(['True Positives', metrics['true_positives']])
        writer.writerow(['False Positives', metrics['false_positives']])
        writer.writerow(['True Negatives', metrics['true_negatives']])
        writer.writerow(['False Negatives', metrics['false_negatives']])
        writer.writerow(['Total Pairs', metrics['total_pairs']])

    print(f"\nResults saved to {output_file}")


def save_error_pairs(pairs_list, output_file, error_type):
    """Save false positive or false negative pairs to CSV."""
    if not pairs_list:
        print(f"No {error_type} to save")
        return

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id1', 'id2', 'alias1_full', 'alias2_full'])
        for pair in pairs_list:
            writer.writerow([
                pair['id1'],
                pair['id2'],
                pair['alias1_full'],
                pair['alias2_full']
            ])

    print(f"{error_type.capitalize()} saved to {output_file} ({len(pairs_list)} pairs)")


def main():
    parser = argparse.ArgumentParser(
        description='Evaluate ALFAA algorithm against ground truth data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script evaluates ALFAA algorithm using World of Code (WoC) for identity resolution.

Ground Truth Formats:
  1. Matrix format (e.g., Gambit data):
     --developers <aliases.csv> --groundtruth <matrix.csv>

  2. Pairwise format (e.g., ALFAA data):
     --pairwise <pairwise.csv>

  3. DRE format (DREUser.csv):
     --dre <DREUser.csv>

  4. Wiese format (apache.community):
     --wiese <apache.community>
     Optional: --asf-developers <AsfDevelopers.txt> (input data with names)

WORKFLOW:
  1. Load ground truth data
  2. Extract unique aliases and save to file (configurable with --woc-input)
  3. YOU MUST: Run World of Code on these aliases and provide results via --woc-output
  4. Parse WoC output and build clusters
  5. Evaluate against ground truth

WoC OUTPUT FORMAT:
  Each line should be: "searched name <email>;assigned name <email>"
  Example: "John Doe <john@example.com>;John A. Doe <john@example.com>"

EXAMPLES:
  # Pairwise format - Prepare input for WoC lookup
  python evaluate_alfaa_groundtruth.py \\
    --pairwise dataset_OpenStack_Amreen/crossRater0.csv \\
    --prepare-only \\
    --woc-input woc_input.txt

  # Matrix format - Prepare input for WoC lookup
  python evaluate_alfaa_groundtruth.py \\
    --developers dataset_GnomeGTK_Gote/gnome_gtk_DEVELOPERS.csv \\
    --groundtruth dataset_GnomeGTK_Gote/gnome_gtk_MANUAL_GROUNDTRUTH.csv \\
    --prepare-only \\
    --woc-input woc_input_gnome.txt

  # Full evaluation after getting WoC results (pairwise format)
  python evaluate_alfaa_groundtruth.py \\
    --pairwise dataset_OpenStack_Amreen/crossRater0.csv \\
    --woc-output woc_results.txt \\
    --output results_alfaa.csv

  # Full evaluation after getting WoC results (matrix format)
  python evaluate_alfaa_groundtruth.py \\
    --developers dataset_GnomeGTK_Gote/gnome_gtk_DEVELOPERS.csv \\
    --groundtruth dataset_GnomeGTK_Gote/gnome_gtk_MANUAL_GROUNDTRUTH.csv \\
    --woc-output woc_results.txt \\
    --output results_alfaa_gnome.csv

  # DRE format - Prepare input for WoC lookup
  python evaluate_alfaa_groundtruth.py \\
    --dre dataset_DRE_Amreen/DREUser.csv \\
    --prepare-only \\
    --woc-input woc_input_dre.txt

  # Full evaluation after getting WoC results (DRE format)
  python evaluate_alfaa_groundtruth.py \\
    --dre dataset_DRE_Amreen/DREUser.csv \\
    --woc-output woc_results.txt \\
    --output results_alfaa_dre.csv

  # Wiese format (emails only) - Prepare input for WoC lookup
  python evaluate_alfaa_groundtruth.py \\
    --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \\
    --prepare-only \\
    --woc-input wiese_data/woc-a-input-wiese-groundtruth.txt

  # Wiese format with input data (name-email pairs) - Prepare input for WoC lookup
  python evaluate_alfaa_groundtruth.py \\
    --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \\
    --asf-developers dataset_ASF_Wiese/masterDegreeAnalisys/resources/AsfDevelopers.txt \\
    --prepare-only \\
    --woc-input wiese_data/woc-a-input-wiese-asf-groundtruth.txt

  # Full evaluation after getting WoC results (Wiese format, emails only)
  python evaluate_alfaa_groundtruth.py \\
    --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \\
    --woc-output wiese_data/woc-a-input-wiese-groundtruth-output.txt \\
    --output results_alfaa_wiese.csv

  # Full evaluation after getting WoC results (Wiese format with AsfDevelopers)
  python evaluate_alfaa_groundtruth.py \\
    --wiese dataset_ASF_Wiese/masterDegreeAnalisys/datasets/apache.community \\
    --asf-developers dataset_ASF_Wiese/masterDegreeAnalisys/resources/AsfDevelopers.txt \\
    --woc-output wiese_data/woc-a-input-wiese-asf-groundtruth-output.txt \\
    --output results_alfaa_wiese_asf.csv
        """
    )

    parser.add_argument('--developers', type=str, help='CSV file with alias list (for matrix format)')
    parser.add_argument('--groundtruth', type=str, help='CSV file with ground truth matrix (for matrix format)')
    parser.add_argument('--pairwise', type=str, help='CSV file with pairwise ground truth (for pairwise format)')
    parser.add_argument('--dre', type=str, help='CSV file with DRE ground truth (DREUser.csv format)')
    parser.add_argument('--wiese', type=str, help='Text file with Wiese ground truth (apache.community format)')
    parser.add_argument('--asf-developers', type=str, dest='asf_developers',
                       help='Optional input data file for Wiese format (Name <email> pairs)')
    parser.add_argument('--woc-input', type=str, default='woc_input.txt',
                       help='Output file for WoC input aliases (default: woc_input.txt)')
    parser.add_argument('--woc-output', type=str,
                       help='File containing World of Code output (one assignment per line)')
    parser.add_argument('--prepare-only', action='store_true',
                       help='Only prepare WoC input file, do not evaluate')
    parser.add_argument('--output', type=str,
                       help='Output CSV file for results')

    args = parser.parse_args()

    # Validate arguments
    if args.wiese:
        if args.developers or args.groundtruth or args.pairwise or args.dre:
            print("Error: Cannot use --wiese with other format options")
            sys.exit(1)
        ground_truth_format = 'wiese'
    elif args.dre:
        if args.developers or args.groundtruth or args.pairwise:
            print("Error: Cannot use --dre with other format options")
            sys.exit(1)
        ground_truth_format = 'dre'
    elif args.pairwise:
        if args.developers or args.groundtruth:
            print("Error: Cannot use --pairwise with --developers or --groundtruth")
            sys.exit(1)
        ground_truth_format = 'pairwise'
    elif args.developers and args.groundtruth:
        ground_truth_format = 'matrix'
    else:
        print("Error: Must specify either --wiese OR --dre OR --pairwise OR (--developers and --groundtruth)")
        parser.print_help()
        sys.exit(1)

    # Change to evaluation directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Load ground truth
    print(f"Loading ground truth ({ground_truth_format} format)...")
    if ground_truth_format == 'matrix':
        aliases_df, pairs = GroundTruthLoader.load_matrix_format(args.developers, args.groundtruth)
        print(f"Loaded {len(aliases_df)} aliases and {len(pairs)} pairs")
    elif ground_truth_format == 'pairwise':
        aliases_df, pairs = GroundTruthLoader.load_pairwise_format(args.pairwise)
        print(f"Loaded {len(aliases_df)} unique aliases and {len(pairs)} pairs")
    elif ground_truth_format == 'dre':
        aliases_df, pairs = GroundTruthLoader.load_dre_format(args.dre)
        print(f"Loaded {len(aliases_df)} aliases and {len(pairs)} pairs")
        print(f"Ground truth clusters: {len(aliases_df['dre_id'].unique())}")
    else:  # wiese
        aliases_df, pairs = GroundTruthLoader.load_wiese_format(args.wiese, args.asf_developers)
        print(f"Loaded {len(aliases_df)} aliases and {len(pairs)} pairs")
        print(f"Ground truth clusters: {len(aliases_df['cluster_id'].unique())}")

    # Prepare WoC input
    woc_input_file = args.woc_input
    alias_list = prepare_woc_input(aliases_df, woc_input_file)

    if args.prepare_only:
        print("\n" + "="*70)
        print("NEXT STEPS:")
        print("="*70)
        print(f"1. Use the aliases in '{woc_input_file}' to query World of Code")
        print("2. WoC should return output in format:")
        print("   'searched name <email>;assigned name <assigned email>'")
        print("3. Save WoC output to a file (e.g., woc_results.txt)")
        print("4. Run this script again with --woc-output flag:")
        print(f"   python {os.path.basename(__file__)} \\")
        if ground_truth_format == 'matrix':
            print(f"     --developers {args.developers} \\")
            print(f"     --groundtruth {args.groundtruth} \\")
        elif ground_truth_format == 'pairwise':
            print(f"     --pairwise {args.pairwise} \\")
        elif ground_truth_format == 'dre':
            print(f"     --dre {args.dre} \\")
        else:  # wiese
            print(f"     --wiese {args.wiese} \\")
        print(f"     --woc-output woc_results.txt \\")
        print(f"     --output results_alfaa.csv")
        print("="*70)
        return

    # Check if WoC output is provided
    if not args.woc_output:
        print("\nError: --woc-output is required for evaluation")
        print("Use --prepare-only to just generate the input file for World of Code")
        sys.exit(1)

    # ============================================================================
    # TODO: World of Code Lookup
    # ============================================================================
    # At this point, the list of aliases to query is ready in 'alias_list'
    # Each alias is in format: "Name <email>"
    #
    # INSTRUCTIONS:
    # 1. Query World of Code (WoC) for each alias in 'alias_list'
    # 2. WoC should return assignments in this format:
    #    "searched name <searched email>;assigned name <assigned email>"
    #
    # 3. Provide the results via --woc-output file, where each line follows
    #    the format above
    #
    # Example WoC output format:
    #   John Doe <john@example.com>;John A. Doe <john@example.com>
    #   Jane Smith <jane@test.org>;Jane Smith <jane@test.org>
    #   Bob Johnson <bob@company.com>;Robert Johnson <bob@company.com>
    # ============================================================================

    # Load and parse WoC output
    print(f"\nLoading World of Code output from {args.woc_output}...")
    try:
        with open(args.woc_output, 'r', encoding='latin-1') as f:
            woc_output_lines = f.readlines()

    except FileNotFoundError:
        print(f"Error: WoC output file not found: {args.woc_output}")
        sys.exit(1)

    woc_mapping = parse_woc_output(woc_output_lines)
    print(f"Parsed {len(woc_mapping)} WoC assignments")

    # Debug: Show sample mappings
    print("\nSample WoC mappings (first 5):")
    for i, (searched, assigned) in enumerate(list(woc_mapping.items())[:5]):
        print(f"  {searched} -> {assigned}")

    # Build clusters from WoC
    print("\nBuilding clusters from World of Code assignments...")
    alias_to_cluster = build_clusters_from_woc(aliases_df, woc_mapping)
    num_clusters = len(set(alias_to_cluster.values()))
    print(f"ALFAA (via WoC) created {num_clusters} clusters")

    # Debug: Show cluster distribution
    from collections import Counter
    cluster_sizes = Counter(alias_to_cluster.values())
    print(f"Cluster size distribution: {len([c for c, size in cluster_sizes.items() if size > 1])} clusters with >1 alias")
    print(f"Largest cluster has {max(cluster_sizes.values())} aliases")

    # Evaluate
    print("\nEvaluating predictions...")
    metrics = evaluate_predictions(pairs, alias_to_cluster, aliases_df)

    # Print results
    print_results(metrics)

    # Save results if output file specified
    if args.output:
        save_results(metrics, args.output)

        # Generate false positive and false negative file names
        output_dir = os.path.dirname(args.output) or '.'
        output_base = os.path.basename(args.output).replace('.csv', '')
        fp_path = os.path.join(output_dir, f'false_positives_{output_base}.csv')
        fn_path = os.path.join(output_dir, f'false_negatives_{output_base}.csv')

        # Save error pairs
        save_error_pairs(metrics['false_positive_pairs'], fp_path, 'false positives')
        save_error_pairs(metrics['false_negative_pairs'], fn_path, 'false negatives')


if __name__ == "__main__":
    main()
